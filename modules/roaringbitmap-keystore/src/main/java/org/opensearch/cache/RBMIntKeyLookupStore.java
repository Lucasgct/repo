/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;


import org.opensearch.common.cache.tier.keystore.KeyLookupStore;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.roaringbitmap.RoaringBitmap;

/**
 * This class implements KeyLookupStore using a roaring bitmap with a modulo applied to values.
 * The modulo increases the density of values, which makes RBMs more memory-efficient. The recommended modulo is ~2^28.
 * It also maintains a hash set of values which have had collisions. Values which haven't had collisions can be
 * safely removed from the store. The fraction of collided values should be low,
 * about 0.5% for a store with 10^7 values and a modulo of 2^28.
 * The store estimates its memory footprint and will stop adding more values once it reaches its memory cap.
 */
public class RBMIntKeyLookupStore implements KeyLookupStore<Integer> {
    /**
     * An enum representing modulo values for use in the keystore
     */
    public enum KeystoreModuloValue {
        NONE(0), // No modulo applied
        TWO_TO_THIRTY_ONE((int) Math.pow(2, 31)),
        TWO_TO_TWENTY_NINE((int) Math.pow(2, 29)), // recommended value
        TWO_TO_TWENTY_EIGHT((int) Math.pow(2, 28)),
        TWO_TO_TWENTY_SIX((int) Math.pow(2, 26));

        private final int value;

        private KeystoreModuloValue(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    protected final int modulo;
    protected final int modulo_bitmask;
    // Since our modulo is always a power of two we can optimize it by ANDing with a particular bitmask
    KeyStoreStats stats;
    protected RoaringBitmap rbm;
    private HashMap<Integer, CounterMetric> collidedIntCounters;
    private HashMap<Integer, HashSet<Integer>> removalSets;
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();
    private long mostRecentByteEstimate;

    // Refresh size estimate every X new elements. Refreshes use the RBM's internal size estimator, which takes ~0.01 ms,
    // so we don't want to do it on every get(), and it doesn't matter much if there are +- 10000 keys in this store
    // in terms of storage impact
    static final int REFRESH_SIZE_EST_INTERVAL = 10_000;


    // Use this constructor to specify memory cap with default modulo = 2^28, which we found in experiments
    // to be the best tradeoff between lower memory usage and risk of collisions
    public RBMIntKeyLookupStore(long memSizeCapInBytes) {
        this(KeystoreModuloValue.TWO_TO_TWENTY_EIGHT, memSizeCapInBytes);
    }

    // Use this constructor to specify memory cap and modulo
    public RBMIntKeyLookupStore(KeystoreModuloValue moduloValue, long memSizeCapInBytes) {
        this.modulo = moduloValue.getValue();
        if (modulo > 0) {
            this.modulo_bitmask = modulo - 1; // keep last log_2(modulo) bits
        } else {
            this.modulo_bitmask = -1; // -1 in twos complement is all ones -> includes all bits -> same as no modulo
        }
        this.stats = new KeyStoreStats(memSizeCapInBytes);
        this.rbm = new RoaringBitmap();
        this.collidedIntCounters = new HashMap<>();
        this.removalSets = new HashMap<>();
        this.mostRecentByteEstimate = 0L;
    }

    private int transform(int value) {
        return value & modulo_bitmask;
    }

    private void handleCollisions(int transformedValue) {
        stats.numCollisions.inc();
        CounterMetric numCollisions = collidedIntCounters.get(transformedValue);
        if (numCollisions == null) { // First time the transformedValue has had a collision
            numCollisions = new CounterMetric();
            numCollisions.inc(2); // initialize to 2, since the first collision means 2 keys have collided
            collidedIntCounters.put(transformedValue, numCollisions);
        } else {
            numCollisions.inc();
        }
    }

    private boolean shouldUpdateByteEstimate() {
        return getSize() % REFRESH_SIZE_EST_INTERVAL == 0;
    }

    private boolean isAtCapacityLimit() {
        return getMemorySizeCapInBytes() > 0 && mostRecentByteEstimate > getMemorySizeCapInBytes();
    }

    @Override
    public boolean add(Integer value) {
        if (value == null) {
            return false;
        }
        stats.numAddAttempts.inc();

        if (shouldUpdateByteEstimate()) {
            mostRecentByteEstimate = computeMemorySizeInBytes();
        }
        if (isAtCapacityLimit()) {
            stats.atCapacity.set(true);
            return false;
        }
        int transformedValue = transform(value);

        writeLock.lock();
        try {
            if (!rbm.contains(transformedValue)) {
                rbm.add(transformedValue);
                stats.size.inc();
                return true;
            }
            // If the value is already pending removal, take it out of the removalList
            HashSet<Integer> removalSet = removalSets.get(transformedValue);
            if (removalSet != null) {
                removalSet.remove(value);
                // Don't increment the counter - this is handled by handleCollisions() later
                if (removalSet.isEmpty()) {
                    removalSets.remove(transformedValue);
                }
            }

            handleCollisions(transformedValue);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean contains(Integer value) {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            return rbm.contains(transformedValue);
        } finally {
            readLock.unlock();
        }
    }

    public Integer getInternalRepresentation(Integer value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(transform(value));
    }

    /**
     * Attempts to remove a value from the keystore. WARNING: Removing keys which have not been added to the keystore
     * may cause undefined behavior, including future false negatives!!
     * @param value The value to attempt to remove.
     * @return true if the value was removed, false otherwise
     */
    @Override
    public boolean remove(Integer value) {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            if (!rbm.contains(transformedValue)) { // saves additional transform() call
                return false;
            }
            stats.numRemovalAttempts.inc();
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            CounterMetric numCollisions = collidedIntCounters.get(transformedValue);
            if (numCollisions != null) {
                // This transformed value has had a collision before
                HashSet<Integer> removalSet = removalSets.get(transformedValue);
                if (removalSet == null) {
                    // First time a removal has been attempted for this transformed value
                    HashSet<Integer> newRemovalSet = new HashSet<>();
                    newRemovalSet.add(value); // Add the key value, not the transformed value, to the list of attempted removals for this transformedValue
                    removalSets.put(transformedValue, newRemovalSet);
                    numCollisions.dec();
                } else {
                    if (removalSet.contains(value)) {
                        return false; // We have already attempted to remove this value. Do nothing
                    }
                    removalSet.add(value);
                    numCollisions.dec();
                    // If numCollisions has reached zero, we can safely remove all values in removalList
                    if (numCollisions.count() == 0) {
                        removeFromRBM(transformedValue);
                        collidedIntCounters.remove(transformedValue);
                        removalSets.remove(transformedValue);
                        return true;
                    }
                }
                return false;
            }
            // Otherwise, there's not been a collision for this transformedValue, so we can safely remove
            removeFromRBM(transformedValue);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    // Helper fn for remove()
    private void removeFromRBM(int transformedValue) {
        if (!lock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("Write Lock must be held when calling this method");
        }
        rbm.remove(transformedValue);
        stats.size.dec();
        stats.numSuccessfulRemovals.inc();
    }

    @Override
    public int getSize() {
        readLock.lock();
        try {
            return (int) stats.size.count();
        } finally {
            readLock.unlock();
        }
    }

    public int getAddAttempts() {
        return (int) stats.numAddAttempts.count();
    }

    public int getCollisions() {
        return (int) stats.numCollisions.count();
    }

    public boolean isCollision(Integer value1, Integer value2) {
        if (value1 == null || value2 == null) {
            return false;
        }
        return transform(value1) == transform(value2);
    }

    /*
    The built-in RBM size estimator is known to work very badly for randomly-distributed data, like the hashes we will be using.
    See https://github.com/RoaringBitmap/RoaringBitmap/issues/257.
    We ran tests to determine what multiplier you need  to get true size from reported size, as a function of log10(# entries / modulo),
    and found this piecewise linear function was a good approximation across different modulos.
     */
    static double getRBMSizeMultiplier(int numEntries, int modulo) {
        double effectiveModulo = (double) modulo / 2;
        /* This model was created when we used % operator to calculate modulo. This has range (-modulo, modulo).
        Now we have optimized to use a bitmask, which has range [0, modulo). So the number of possible values stored
        is halved. */
        if (modulo == 0) {
            effectiveModulo = Math.pow(2, 32);
        }
        double x = Math.log10((double) numEntries / effectiveModulo);
        if (x < -5) {
            return 7.0;
        }
        if (x < -2.75) {
            return -2.5 * x - 5.5;
        }
        if (x <= 0) {
            return -3.0 / 22.0 * x + 1;
        }
        return 1;
    }

    /**
     * Return the most recent memory size estimate, without updating it.
     * @return the size estimate (bytes)
     */
    @Override
    public long getMemorySizeInBytes() {
        return mostRecentByteEstimate;
    }

    /**
     * Calculate a new memory size estimate. This is somewhat expensive, so we don't call this every time we run get().
     * @return a new size estimate (bytes)
     */
    private long computeMemorySizeInBytes() {
        double multiplier = getRBMSizeMultiplier((int) stats.size.count(), modulo);
        return (long) (rbm.getSizeInBytes() * multiplier);
    }

    @Override
    public long getMemorySizeCapInBytes() {
        return stats.memSizeCapInBytes;
    }

    @Override
    public boolean isFull() {
        return stats.atCapacity.get();
    }

    @Override
    public void regenerateStore(Integer[] newValues) {
        rbm.clear();
        collidedIntCounters = new HashMap<>();
        removalSets = new HashMap<>();
        stats.size = new CounterMetric();
        stats.numAddAttempts = new CounterMetric();
        stats.numCollisions = new CounterMetric();
        stats.numRemovalAttempts = new CounterMetric();
        stats.numSuccessfulRemovals = new CounterMetric();
        for (int i = 0; i < newValues.length; i++) {
            if (newValues[i] != null) {
                add(newValues[i]);
            }
        }
    }

    @Override
    public void clear() {
        regenerateStore(new Integer[] {});
    }

    public int getNumRemovalAttempts() {
        return (int) stats.numRemovalAttempts.count();
    }

    public int getNumSuccessfulRemovals() {
        return (int) stats.numSuccessfulRemovals.count();
    }

    public boolean valueHasHadCollision(Integer value) {
        if (value == null) {
            return false;
        }
        return collidedIntCounters.containsKey(transform(value));
    }

    CounterMetric getNumCollisionsForValue(int value) { // package private for testing
        return collidedIntCounters.get(transform(value));
    }

    HashSet<Integer> getRemovalSetForValue(int value) {
        return removalSets.get(transform(value));
    }

    /**
     * Function to set a new memory size cap.
     * TODO: Integrate this with the tiered caching cluster settings PR once this is raised.
     * @param newMemSizeCap The new cap size.
     */
    protected void setMemSizeCap(ByteSizeValue newMemSizeCap) {
        stats.memSizeCapInBytes = newMemSizeCap.getBytes();
        mostRecentByteEstimate = getMemorySizeInBytes();
        if (mostRecentByteEstimate > getMemorySizeCapInBytes()) {
            stats.atCapacity.set(true);
        }
    }
}

