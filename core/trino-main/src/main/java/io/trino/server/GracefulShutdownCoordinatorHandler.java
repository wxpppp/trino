/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.QueryManager;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GracefulShutdownCoordinatorHandler
        implements GracefulShutdownHandler
{
    private static final Logger log = Logger.get(GracefulShutdownCoordinatorHandler.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ScheduledExecutorService shutdownHandler = newSingleThreadScheduledExecutor(threadsNamed("shutdown-handler-%s"));
    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final StartupStatus status;
    private final QueryManager queryManager;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;
    private final Duration timeout;

    @GuardedBy("this")
    private boolean shutdownRequested;

    @Inject
    public GracefulShutdownCoordinatorHandler(
            StartupStatus status,
            QueryManager queryManager,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this.status = requireNonNull(status, "status is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.gracePeriod = requireNonNull(serverConfig, "serverConfig is null").getGracePeriod();
        this.timeout = serverConfig.getShutdownTimeout();
    }

    @Override
    public synchronized void requestShutdown()
    {
        log.info("Coordinator shutdown requested");
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;

        // mark coordinator as shutting down - new query submissions will be rejected
        status.shutdownStarted();

        // wait for a grace period to start the shutdown sequence
        shutdownHandler.schedule(this::shutdown, gracePeriod.toMillis(), MILLISECONDS);
    }

    private void shutdown()
    {
        log.info("Shutting down coordinator");
        List<BasicQueryInfo> activeQueries = getActiveQueries();

        // At this point no new queries should be queued by the coordinator.
        // Wait for all remaining queries to finish.
        CountDownLatch countDownLatch = new CountDownLatch(activeQueries.size());

        for (BasicQueryInfo queryInfo : activeQueries) {
            queryManager.addStateChangeListener(queryInfo.getQueryId(), newState -> {
                if (newState.isDone()) {
                    countDownLatch.countDown();
                }
            });
        }

        try {
            if (!countDownLatch.await(timeout.toMillis(), MILLISECONDS)) {
                log.info("Shutdown timeout exceeded, %d still active queries being cancelled", getActiveQueries().size());

                // Cancel remaining active queries
                getActiveQueries().stream()
                        .map(BasicQueryInfo::getQueryId)
                        .forEach(queryManager::cancelQuery);
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for all queries to finish");
            currentThread().interrupt();
        }

        Future<?> shutdownFuture = lifeCycleStopper.submit(() -> {
            lifeCycleManager.stop();
            return null;
        });

        // terminate the jvm if life cycle cannot be stopped in a timely manner
        try {
            shutdownFuture.get(LIFECYCLE_STOP_TIMEOUT.toMillis(), MILLISECONDS);
        }
        catch (TimeoutException e) {
            log.warn(e, "Timed out waiting for the life cycle to stop");
        }
        catch (InterruptedException e) {
            log.warn(e, "Interrupted while waiting for the life cycle to stop");
            currentThread().interrupt();
        }
        catch (ExecutionException e) {
            log.warn(e, "Problem stopping the life cycle");
        }

        shutdownAction.onShutdown();
    }

    private List<BasicQueryInfo> getActiveQueries()
    {
        return queryManager.getQueries()
                .stream()
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .collect(toImmutableList());
    }

    @Override
    public synchronized boolean isShutdownRequested()
    {
        return shutdownRequested;
    }
}
