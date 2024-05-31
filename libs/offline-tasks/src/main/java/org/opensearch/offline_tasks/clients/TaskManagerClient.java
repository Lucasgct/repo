/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.clients;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.offline_tasks.task.Task;
import org.opensearch.offline_tasks.task.TaskId;
import org.opensearch.offline_tasks.worker.WorkerNode;

import java.util.List;

/**
 * Client used to interact with Task Store/Queue
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskManagerClient {

    /**
     * Get task from TaskStore/Queue
     *
     * @param taskId TaskId of the task to be retrieved
     * @return Task corresponding to TaskId
     */
    Task getTask(TaskId taskId);

    /**
     * Update task in TaskStore/Queue
     *
     * @param task Task to be updated
     */
    void updateTask(Task task);

    /**
     * Mark task as cancelled.
     * Ongoing Tasks can be cancelled as well if the corresponding worker supports cancellation
     *
     * @param taskId TaskId of the task to be cancelled
     */
    void cancelTask(TaskId taskId);

    /**
     * Assign Task to a particular WorkerNode. This ensures no 2 worker Nodes work on the same task.
     * This API can be used in both pull and push models of task assignment.
     *
     * @param taskId TaskId of the task to be assigned
     * @param node WorkerNode task is being assigned to
     * @return true if task is claimed successfully, false otherwise
     */
    boolean assignTask(TaskId taskId, WorkerNode node);

    /**
     * List all tasks applying all the filters present in listTaskRequest
     *
     * @param listTaskRequest ListTaskRequest
     * @return list of all the task matching the filters in listTaskRequest
     */
    List<Task> listTasks(ListTaskRequest listTaskRequest);

    /**
     * Sends task heart beat to Task Store/Queue
     *
     * @param taskId TaskId of Task to send heartbeat for
     * @param timestamp timestamp of heartbeat to be recorded in TaskStore/Queue
     */
    void sendTaskHeartbeat(TaskId taskId, long timestamp);
}
