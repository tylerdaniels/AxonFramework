/*
 * Copyright (c) 2010-2019. Axon Framework
 *
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

package org.axonframework.axonserver.connector.query.subscription;

import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.QueryUpdateCompleteExceptionally;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.stub.StreamObserver;
import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.ErrorCode;
import org.axonframework.axonserver.connector.util.FlowControllingStreamObserver;
import org.axonframework.common.AxonException;
import org.axonframework.common.Registration;
import org.axonframework.queryhandling.DefaultSubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryBackpressure;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest.newBuilder;
import static java.util.Optional.ofNullable;

/**
 * A {@link SubscriptionQueryResult} that emits initial response and update when subscription query response message is
 * received.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class AxonServerSubscriptionQueryResultCopy
        implements Supplier<SubscriptionQueryResult<QueryResponse, QueryUpdate>> {

    private final Logger logger = LoggerFactory.getLogger(AxonServerSubscriptionQueryResultCopy.class);
    private final SubscriptionQuery subscriptionQuery;
    private final Function<StreamObserver<SubscriptionQueryResponse>, StreamObserver<SubscriptionQueryRequest>> openStreamFn;
    private final AxonServerConfiguration configuration;
    private final int bufferSize;
    private final SubscriptionQueryBackpressure backPressure;
    private final Runnable onDispose;

    /**
     * Instantiate a {@link AxonServerSubscriptionQueryResultCopy} which will emit its initial response and the updates
     * of
     * the subscription query.
     *
     * @param subscriptionQuery the {@link SubscriptionQuery} which is sent
     * @param openStreamFn      a {@link Function} used to open the stream results
     * @param configuration     a {@link AxonServerConfiguration} providing the specified flow control settings
     * @param backPressure      the used {@link SubscriptionQueryBackpressure} for the subsequent updates of the
     *                          subscription query
     * @param bufferSize        an {@code int} specifying the buffer size of the updates
     * @param onDispose         a {@link Runnable} which will be {@link Runnable#run()} this subscription query has
     *                          completed (exceptionally)
     */
    public AxonServerSubscriptionQueryResultCopy(
            SubscriptionQuery subscriptionQuery,
            Function<StreamObserver<SubscriptionQueryResponse>, StreamObserver<SubscriptionQueryRequest>> openStreamFn,
            AxonServerConfiguration configuration,
            SubscriptionQueryBackpressure backPressure,
            int bufferSize,
            Runnable onDispose) {

        this.subscriptionQuery = subscriptionQuery;
        this.openStreamFn = openStreamFn;
        this.configuration = configuration;
        this.bufferSize = bufferSize;
        this.backPressure = backPressure;
        this.onDispose = onDispose;
    }

    private FlowControllingStreamObserver<SubscriptionQueryRequest> initFlowControllingStreamObserver(
            StreamObserver<SubscriptionQueryRequest> streamObserver,
            AxonServerConfiguration configuration) {

        Function<FlowControl, SubscriptionQueryRequest> requestMapping =
                flowControl -> newBuilder().setFlowControl(
                        SubscriptionQuery.newBuilder(this.subscriptionQuery)
                                         .setNumberOfPermits(flowControl.getPermits())
                ).build();

        FlowControllingStreamObserver<SubscriptionQueryRequest> observer = new FlowControllingStreamObserver<>(
                streamObserver,
                configuration,
                requestMapping,
                t -> false);

        observer.sendInitialPermits();
        return observer;
    }


//    private void updateError(Throwable t) {
//        try {
//            updateMessageFluxSink.error(t);
//        } catch (Exception e) {
//            logger.warn("Problem signaling updates error.", e);
//            updateMessageFluxSink.complete();
//        }
//    }


//    private void initialResultError(Throwable t) {
//        try {
//            ofNullable(initialResultSink.get()).ifPresent(sink -> sink.error(t));
//        } catch (Exception e) {
//            logger.warn("Problem signaling initial result error.", e);
//        }
//    }

    @Override
    public SubscriptionQueryResult<QueryResponse, QueryUpdate> get() {
        AtomicReference<MonoSink<QueryResponse>> initialResultSink = new AtomicReference<>();
        EmitterProcessor<QueryUpdate> processor = EmitterProcessor.create(bufferSize);
        FluxSink<QueryUpdate> updateMessageFluxSink = processor.sink(backPressure.getOverflowStrategy());
        AtomicReference<FlowControllingStreamObserver<SubscriptionQueryRequest>> requestObserver = new AtomicReference<>();
        requestObserver.set(initFlowControllingStreamObserver(openStreamFn
                                                                      .apply(new StreamObserver<SubscriptionQueryResponse>() {

                                                                          @Override
                                                                          public void onNext(
                                                                                  SubscriptionQueryResponse response) {
                                                                              requestObserver.get().markConsumed(1);
                                                                              switch (response.getResponseCase()) {
                                                                                  case INITIAL_RESULT:
                                                                                      ofNullable(initialResultSink
                                                                                                         .get())
                                                                                              .ifPresent(sink -> sink
                                                                                                      .success(response.getInitialResult()));
                                                                                      break;
                                                                                  case UPDATE:
                                                                                      updateMessageFluxSink.next(
                                                                                              response.getUpdate());
                                                                                      break;
                                                                                  case COMPLETE:
                                                                                      ofNullable(initialResultSink
                                                                                                         .get())
                                                                                              .ifPresent(sink -> sink
                                                                                                      .error(new IllegalStateException(
                                                                                                              "Subscription Completed")));
                                                                                      updateMessageFluxSink.complete();
                                                                                      requestObserver.get()
                                                                                                     .onCompleted();
                                                                                      onDispose.run();
                                                                                      break;
                                                                                  case COMPLETE_EXCEPTIONALLY:
                                                                                      QueryUpdateCompleteExceptionally exceptionally = response
                                                                                              .getCompleteExceptionally();
                                                                                      AxonException t = ErrorCode
                                                                                              .getFromCode(exceptionally
                                                                                                                   .getErrorCode())
                                                                                              .convert(exceptionally
                                                                                                               .getErrorMessage());
                                                                                      ofNullable(initialResultSink
                                                                                                         .get())
                                                                                              .ifPresent(sink -> sink
                                                                                                      .error(t));
                                                                                      updateMessageFluxSink.error(t);
                                                                                      requestObserver.get()
                                                                                                     .onCompleted();
                                                                                      onDispose.run();
                                                                                      break;
                                                                              }
                                                                          }

                                                                          @Override
                                                                          public void onError(Throwable t) {
                                                                              ofNullable(initialResultSink.get())
                                                                                      .ifPresent(sink -> sink.error(t));
                                                                              updateMessageFluxSink.error(t);
                                                                              onDispose.run();
                                                                          }

                                                                          @Override
                                                                          public void onCompleted() {
                                                                              ofNullable(initialResultSink.get())
                                                                                      .ifPresent(sink -> sink
                                                                                              .error(new IllegalStateException(
                                                                                                      "Subscription Completed")));
                                                                              updateMessageFluxSink.complete();
                                                                              onDispose.run();
                                                                          }
                                                                      }), configuration));


        requestObserver.get().onNext(newBuilder().setSubscribe(this.subscriptionQuery).build());
        Mono<QueryResponse> mono = Mono.create(sink -> {
            initialResultSink.set(sink);
            //requires to AxonServer the initial result
            requestObserver.get().onNext(newBuilder().setGetInitialResult(subscriptionQuery).build());
        });

        Registration onCancel = () -> {
            ofNullable(initialResultSink.get()).ifPresent(MonoSink::success);
            updateMessageFluxSink.complete();
            requestObserver.get().onCompleted();
            onDispose.run();
            return true;
        };

        return new DefaultSubscriptionQueryResult<>(mono, processor.replay().autoConnect(), onCancel);
    }
}
