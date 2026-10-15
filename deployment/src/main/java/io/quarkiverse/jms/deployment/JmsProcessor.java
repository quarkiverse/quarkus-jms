package io.quarkiverse.jms.deployment;

import static io.quarkiverse.jms.deployment.DotNames.APPLICATION_SCOPED;
import static io.quarkiverse.jms.deployment.DotNames.IDENTIFIER;
import static io.quarkiverse.jms.deployment.DotNames.JACKSON_JMS_CONVERTER;
import static io.quarkiverse.jms.deployment.DotNames.JMS_CONVERTER;
import static io.quarkiverse.jms.deployment.DotNames.JMS_QUEUE;
import static io.quarkiverse.jms.deployment.DotNames.JMS_RESPONSE;
import static io.quarkiverse.jms.deployment.DotNames.JMS_TOPIC;
import static io.quarkiverse.jms.deployment.DotNames.SPRING_JMS_RESPONSE;
import static io.quarkus.arc.processor.DotNames.OBJECT;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.Topic;
import jakarta.jms.XAConnectionFactory;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationTransformation;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.ClassType;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.JmsTopic;
import io.quarkiverse.jms.runtime.BuiltInJmsMessageConverter;
import io.quarkiverse.jms.runtime.JacksonJmsMessageConverter;
import io.quarkiverse.jms.runtime.JmsClientImpl;
import io.quarkiverse.jms.runtime.JmsConnectionFactoryResolver;
import io.quarkiverse.jms.runtime.JmsListenerInvoker;
import io.quarkiverse.jms.runtime.JmsListenerMetadata;
import io.quarkiverse.jms.runtime.JmsListenerRegistryImpl;
import io.quarkiverse.jms.runtime.JmsMdcInstrumentation;
import io.quarkiverse.jms.runtime.JmsMessageConverterResolver;
import io.quarkiverse.jms.runtime.JmsRecorder;
import io.quarkiverse.jms.spi.deployment.JmsListenerDiscoveredBuildItem;
import io.quarkiverse.jms.spi.deployment.JmsListenerManagedBuildItem;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.AnnotationsTransformerBuildItem;
import io.quarkus.arc.deployment.CustomScopeAnnotationsBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.SyntheticBeansRuntimeInitBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.execannotations.ExecutionModelAnnotationsAllowedBuildItem;
import io.quarkus.gizmo.AssignableResultHandle;
import io.quarkus.gizmo.BranchResult;
import io.quarkus.gizmo.BytecodeCreator;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.smallrye.health.deployment.spi.HealthBuildItem;

class JmsProcessor {

    private static final Logger LOG = Logger.getLogger(JmsProcessor.class);

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("jms");
    }

    @BuildStep
    ExecutionModelAnnotationsAllowedBuildItem allowVirtualThreadsOnListeners() {
        return new ExecutionModelAnnotationsAllowedBuildItem(
                method -> method.hasDeclaredAnnotation(DotNames.JMS_LISTENER)
                        || method.hasDeclaredAnnotation(DotNames.JMS_LISTENERS));
    }

    @BuildStep
    void registerBeans(BuildProducer<AdditionalBeanBuildItem> beans, Capabilities capabilities) {
        var builder = AdditionalBeanBuildItem.builder()
                .addBeanClasses(
                        JmsClientImpl.class,
                        JmsListenerRegistryImpl.class,
                        JmsConnectionFactoryResolver.class,
                        BuiltInJmsMessageConverter.class,
                        JmsMessageConverterResolver.class,
                        JmsMdcInstrumentation.class);
        if (capabilities.isPresent(Capability.JACKSON)) {
            builder.addBeanClasses(JacksonJmsMessageConverter.class);
        }
        beans.produce(builder.setUnremovable().build());
    }

    @BuildStep
    void registerMicrometerInstrumentation(BuildProducer<AdditionalBeanBuildItem> beans, Capabilities capabilities) {
        if (capabilities.isPresent(Capability.METRICS)) {
            beans.produce(AdditionalBeanBuildItem.builder()
                    .addBeanClass("io.quarkiverse.jms.runtime.instrumentation.JmsMetricsInstrumentation")
                    .setUnremovable()
                    .build());
        }
    }

    @BuildStep
    void registerOpenTelemetryInstrumentation(BuildProducer<AdditionalBeanBuildItem> beans, Capabilities capabilities) {
        if (capabilities.isPresent(Capability.OPENTELEMETRY_TRACER)) {
            beans.produce(AdditionalBeanBuildItem.builder()
                    .addBeanClass("io.quarkiverse.jms.runtime.instrumentation.JmsTracingInstrumentation")
                    .setUnremovable()
                    .build());
        }
    }

    @BuildStep
    void discoverListeners(CombinedIndexBuildItem index,
            BuildProducer<JmsListenerBuildItem> listeners,
            BuildProducer<UnremovableBeanBuildItem> unremovable) {

        List<AnnotationInstance> allAnnotations = new ArrayList<>(
                index.getIndex().getAnnotations(DotNames.JMS_LISTENER));
        for (AnnotationInstance container : index.getIndex().getAnnotations(DotNames.JMS_LISTENERS)) {
            AnnotationValue nestedValue = container.value();
            if (nestedValue != null) {
                for (AnnotationInstance nested : nestedValue.asNestedArray()) {
                    allAnnotations.add(AnnotationInstance.create(nested.name(),
                            container.target(), nested.values()));
                }
            }
        }

        for (AnnotationInstance annotation : allAnnotations) {
            if (annotation.target().kind() != AnnotationTarget.Kind.METHOD) {
                continue;
            }
            MethodInfo method = annotation.target().asMethod();

            // need at least one parameter
            if (method.parametersCount() == 0) {
                throw new DeploymentException("@JmsListener method " + method.declaringClass().name() + "." +
                        method.name() + " must have at least one parameter");
            }

            String destination = annotation.value().asString();
            boolean topic = getBooleanValue(annotation, "topic", false);
            String selector = getStringValue(annotation, "selector", "");
            String connectionFactory = getStringValue(annotation, "connectionFactory", "");
            int concurrency = getIntValue(annotation, "concurrency", 1);
            int maxConcurrency = getIntValue(annotation, "maxConcurrency", -1);
            String subscription = getStringValue(annotation, "subscription", "");

            AnnotationValue ackValue = annotation.value("acknowledge");
            String acknowledgeMode = ackValue != null ? ackValue.asEnum() : "AUTO";

            boolean runOnVirtualThread = method.hasAnnotation(DotNames.RUN_ON_VIRTUAL_THREAD)
                    || method.declaringClass().hasAnnotation(DotNames.RUN_ON_VIRTUAL_THREAD);

            List<JmsListenerBuildItem.Parameter> parameters = classifyParameters(method);

            // Check @SendTo
            AnnotationInstance sendToAnn = method.annotation(DotNames.SEND_TO);
            String sendToDestination = null;
            boolean sendToTopic = false;
            if (sendToAnn != null) {
                String sendToValue = sendToAnn.value() != null ? sendToAnn.value().asString() : "";
                sendToDestination = sendToValue.isEmpty() ? null : sendToValue;
                sendToTopic = sendToAnn.value("topic") != null && sendToAnn.value("topic").asBoolean();
            }

            String returnType = method.returnType().name().toString();
            String methodRef = method.declaringClass().name() + "." + method.name();

            if (maxConcurrency > 0 && maxConcurrency < concurrency) {
                throw new DeploymentException("@JmsListener method " + methodRef
                        + ": maxConcurrency (" + maxConcurrency + ") must be >= concurrency (" + concurrency + ")");
            }
            if (sendToAnn != null && "void".equals(returnType)) {
                throw new DeploymentException("@JmsListener method " + methodRef
                        + ": @SendTo requires a non-void return type");
            }
            if (!topic && !subscription.isEmpty()) {
                LOG.warnf("@JmsListener method %s: 'subscription' is ignored on queue listeners", methodRef);
            }

            String payloadType = parameters.stream()
                    .filter(p -> p instanceof JmsListenerBuildItem.Parameter.Payload)
                    .map(JmsListenerBuildItem.Parameter::type)
                    .findFirst()
                    .orElse(method.parameterType(0).name().toString());

            var methodInfo = new JmsListenerBuildItem.MethodInfo(
                    method.declaringClass().name().toString(),
                    method.name(),
                    parameters,
                    payloadType,
                    returnType);

            listeners.produce(new JmsListenerBuildItem(
                    methodInfo,
                    destination,
                    topic,
                    selector,
                    connectionFactory,
                    concurrency,
                    maxConcurrency,
                    subscription,
                    acknowledgeMode,
                    runOnVirtualThread,
                    sendToDestination,
                    sendToTopic));

            unremovable.produce(UnremovableBeanBuildItem.beanClassNames(method.declaringClass().name().toString()));

            LOG.debugf("Discovered @JmsListener: %s.%s -> %s '%s'",
                    method.declaringClass().name(), method.name(),
                    topic ? "topic" : "queue", destination);
        }
    }

    @BuildStep
    AnnotationsTransformerBuildItem transformBeanScope(CustomScopeAnnotationsBuildItem scopes) {
        return new AnnotationsTransformerBuildItem(AnnotationTransformation.forClasses()
                .transform(ctx -> {
                    ClassInfo clazz = ctx.declaration().asClass();
                    if (scopes.isScopeIn(ctx.annotations())) {
                        return;
                    }
                    if (clazz.annotationsMap().containsKey(DotNames.JMS_LISTENER)
                            || clazz.annotationsMap().containsKey(DotNames.JMS_LISTENERS)) {
                        ctx.add(AnnotationInstance.builder(APPLICATION_SCOPED).build());
                    }
                }));
    }

    @BuildStep
    void generateInvokers(
            CombinedIndexBuildItem combinedIndex,
            Capabilities capabilities,
            List<JmsListenerBuildItem> listeners,
            BuildProducer<GeneratedClassBuildItem> generatedClasses,
            BuildProducer<ReflectiveClassBuildItem> reflective,
            BuildProducer<JmsListenerDiscoveredBuildItem> discovered) {

        if (listeners.isEmpty()) {
            return;
        }

        boolean hasCustomConverters = hasCustomConverters(combinedIndex);
        boolean jacksonAvailable = capabilities.isPresent(Capability.JACKSON);

        ClassOutput classOutput = (name, data) -> {
            generatedClasses.produce(new GeneratedClassBuildItem(true, name, data));
        };

        for (JmsListenerBuildItem listener : listeners) {
            String invokerClassName = generateInvoker(classOutput, listener, hasCustomConverters, jacksonAvailable);
            reflective.produce(ReflectiveClassBuildItem.builder(invokerClassName)
                    .constructors(true).build());

            discovered.produce(new JmsListenerDiscoveredBuildItem(
                    listener.method().beanClassName(),
                    listener.method().methodName(),
                    listener.getDestination(),
                    listener.isTopic(),
                    listener.getSelector(),
                    listener.getConnectionFactory(),
                    listener.getConcurrency(),
                    listener.getSubscription(),
                    listener.getAcknowledgeMode(),
                    listener.isRunOnVirtualThread(),
                    listener.method().payloadType(),
                    invokerClassName));
        }
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    @Consume(SyntheticBeansRuntimeInitBuildItem.class)
    void recordListeners(JmsRecorder recorder,
            List<JmsListenerBuildItem> listeners,
            List<JmsListenerManagedBuildItem> managed) {

        if (listeners.isEmpty()) {
            return;
        }

        Set<String> managedKeys = new HashSet<>();
        for (JmsListenerManagedBuildItem m : managed) {
            managedKeys.add(m.getBeanClassName() + "#" + m.getMethodName());
        }

        List<JmsListenerMetadata> metadataList = new ArrayList<>();
        for (JmsListenerBuildItem listener : listeners) {
            String key = listener.method().beanClassName() + "#" + listener.method().methodName();
            if (!managedKeys.contains(key)) {
                metadataList.add(new JmsListenerMetadata(
                        listener.method().beanClassName(),
                        listener.method().methodName(),
                        listener.getDestination(),
                        listener.isTopic(),
                        listener.getSelector(),
                        listener.getConnectionFactory(),
                        listener.getConcurrency(),
                        listener.getMaxConcurrency(),
                        listener.getSubscription(),
                        listener.getAcknowledgeMode(),
                        listener.isRunOnVirtualThread(),
                        listener.method().invokerClassName(),
                        listener.method().payloadType()));
            } else {
                LOG.debugf("Listener %s.%s is managed by an external provider, skipping container activation",
                        listener.method().beanClassName(), listener.method().methodName());
            }
        }

        recorder.initializeListenerContainer(metadataList);
    }

    @BuildStep
    HealthBuildItem enableHealth(Capabilities capabilities, JmsBuildTimeConfig config) {
        if (capabilities.isPresent(Capability.SMALLRYE_HEALTH) && config.healthEnabled()) {
            return new HealthBuildItem("io.quarkiverse.jms.runtime.JmsHealthCheck", true);
        }
        return null;
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void applyConnectionFactoryWrapper(JmsRecorder recorder,
            Optional<io.quarkiverse.jms.spi.deployment.ConnectionFactoryWrapperBuildItem> wrapperNew,
            Optional<io.quarkus.jms.spi.deployment.ConnectionFactoryWrapperBuildItem> wrapperLegacy) {
        if (wrapperNew.isPresent()) {
            recorder.setConnectionFactoryWrapper(wrapperNew.get().getWrapper());
        } else if (wrapperLegacy.isPresent()) {
            recorder.setConnectionFactoryWrapper(wrapperLegacy.get().getWrapper());
        }
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerXaConnectionFactory(JmsRecorder recorder,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans) {
        syntheticBeans.produce(SyntheticBeanBuildItem
                .configure(XAConnectionFactory.class)
                .scope(ApplicationScoped.class)
                .setRuntimeInit()
                .defaultBean()
                .supplier(recorder.xaConnectionFactorySupplier())
                .done());
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerDestinations(CombinedIndexBuildItem combinedIndex, JmsRecorder recorder,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans) {
        Set<String> registered = new HashSet<>();

        for (var injectionPoint : combinedIndex.getIndex().getAnnotations(IDENTIFIER)) {
            Type injectedType;
            if (injectionPoint.target().kind() == AnnotationTarget.Kind.FIELD) {
                injectedType = injectionPoint.target().asField().type();
            } else if (injectionPoint.target().kind() == AnnotationTarget.Kind.METHOD_PARAMETER) {
                var param = injectionPoint.target().asMethodParameter();
                injectedType = param.method().parameterType(param.position());
            } else {
                continue;
            }
            DotName typeName = injectedType.name();
            String name = injectionPoint.value().asString();
            DotName messageTypeName = extractMessageType(injectedType);

            if (JMS_QUEUE.equals(typeName) && registered.add("queue:" + name)) {
                Type paramType = createParameterizedType(JMS_QUEUE, messageTypeName);
                syntheticBeans.produce(SyntheticBeanBuildItem
                        .configure(JmsQueue.class)
                        .addType(paramType)
                        .scope(ApplicationScoped.class)
                        .setRuntimeInit()
                        .addQualifier().annotation(IDENTIFIER)
                        .addValue("value", name).done()
                        .supplier(recorder.destinationSupplier(name, false, messageTypeName.toString()))
                        .done());
            } else if (JMS_TOPIC.equals(typeName) && registered.add("topic:" + name)) {
                Type paramType = createParameterizedType(JMS_TOPIC, messageTypeName);
                syntheticBeans.produce(SyntheticBeanBuildItem
                        .configure(JmsTopic.class)
                        .addType(paramType)
                        .scope(ApplicationScoped.class)
                        .setRuntimeInit()
                        .addQualifier().annotation(IDENTIFIER)
                        .addValue("value", name).done()
                        .supplier(recorder.destinationSupplier(name, true, messageTypeName.toString()))
                        .done());
            }
        }
    }

    private static DotName extractMessageType(Type fieldType) {
        if (fieldType.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            var args = fieldType.asParameterizedType().arguments();
            if (!args.isEmpty()) {
                return args.get(0).name();
            }
        }
        return OBJECT;
    }

    private static Type createParameterizedType(DotName rawType, DotName typeArgName) {
        Type typeArg = ClassType.create(typeArgName);
        return ParameterizedType.create(rawType, new Type[] { typeArg }, null);
    }

    @BuildStep
    void nativeImage(BuildProducer<ReflectiveClassBuildItem> reflective) {
        reflective.produce(ReflectiveClassBuildItem.builder(
                "jakarta.jms.Message",
                "jakarta.jms.TextMessage",
                "jakarta.jms.BytesMessage",
                "jakarta.jms.MapMessage",
                "jakarta.jms.ObjectMessage",
                "jakarta.jms.StreamMessage")
                .methods(true)
                .build());
    }

    private String generateInvoker(ClassOutput classOutput, JmsListenerBuildItem listener,
            boolean hasCustomConverters, boolean jacksonAvailable) {
        String beanClassName = listener.method().beanClassName();
        String methodName = listener.method().methodName();
        String payloadType = listener.method().payloadType();

        String invokerClassName = beanClassName.replace('$', '_') + "_" + methodName + "_JmsInvoker";

        // Determine which converter class to use for the payload conversion field
        Class<?> converterClass;
        if (hasCustomConverters) {
            converterClass = JmsMessageConverterResolver.class;
        } else if (isBuiltInType(payloadType)) {
            converterClass = BuiltInJmsMessageConverter.class;
        } else if (jacksonAvailable) {
            converterClass = JacksonJmsMessageConverter.class;
        } else {
            converterClass = JmsMessageConverterResolver.class;
        }

        // Determine return type info
        String returnType = listener.method().returnType();
        boolean isQuarkusJmsResponse = JMS_RESPONSE.toString().equals(returnType);
        boolean isSpringJmsResponse = SPRING_JMS_RESPONSE.toString().equals(returnType);
        boolean isJmsResponse = isQuarkusJmsResponse || isSpringJmsResponse;
        boolean hasReply = listener.hasSendTo() || isJmsResponse;
        boolean hasReturn = hasReply && !"void".equals(returnType);

        try (ClassCreator creator = ClassCreator.builder()
                .classOutput(classOutput)
                .className(invokerClassName)
                .interfaces(JmsListenerInvoker.class)
                .build()) {

            // Declare cached fields
            FieldDescriptor beanField = creator.getFieldCreator("bean", beanClassName).getFieldDescriptor();
            FieldDescriptor converterField = creator.getFieldCreator("converter", converterClass)
                    .getFieldDescriptor();
            FieldDescriptor resolverField = hasReturn
                    ? creator.getFieldCreator("resolver", JmsMessageConverterResolver.class).getFieldDescriptor()
                    : null;

            // Generate constructor — resolve beans from Arc and cache in fields
            try (MethodCreator ctor = creator.getMethodCreator("<init>", void.class)) {
                ctor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), ctor.getThis());

                ResultHandle arc = ctor.invokeStaticMethod(
                        MethodDescriptor.ofMethod("io.quarkus.arc.Arc", "container",
                                "io.quarkus.arc.ArcContainer"));

                ctor.writeInstanceField(beanField, ctor.getThis(), resolveBean(ctor, arc, beanClassName));
                ctor.writeInstanceField(converterField, ctor.getThis(), resolveBean(ctor, arc, converterClass));
                if (hasReturn) {
                    ctor.writeInstanceField(resolverField, ctor.getThis(),
                            resolveBean(ctor, arc, JmsMessageConverterResolver.class));
                }

                ctor.returnVoid();
            }

            // Generate invoke(Message, JMSContext) — reads cached fields instead of CDI lookups
            try (MethodCreator invoke = creator.getMethodCreator("invoke",
                    void.class, Message.class, JMSContext.class)) {

                ResultHandle castBean = invoke.readInstanceField(beanField, invoke.getThis());
                ResultHandle castConverter = invoke.readInstanceField(converterField, invoke.getThis());
                ResultHandle message = invoke.getMethodParam(0);
                ResultHandle jmsContext = invoke.getMethodParam(1);

                // Convert payload
                ResultHandle targetClass = invoke.loadClass(payloadType);
                ResultHandle payload = invoke.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(converterClass,
                                "fromMessage", Object.class, Message.class, Class.class),
                        castConverter, message, targetClass);
                ResultHandle castPayload = invoke.checkCast(payload, payloadType);

                // Build method parameter array
                var params = listener.method().parameters();
                List<String> allParamTypes = listener.method().allParameterTypes();
                ResultHandle[] methodArgs = new ResultHandle[params.size()];

                for (int i = 0; i < params.size(); i++) {
                    methodArgs[i] = switch (params.get(i)) {
                        case JmsListenerBuildItem.Parameter.Payload p -> castPayload;
                        case JmsListenerBuildItem.Parameter.RawMessage r -> invoke.checkCast(message, r.type());
                        case JmsListenerBuildItem.Parameter.Header h -> generateHeaderExtraction(invoke, message, h);
                    };
                }

                if (hasReturn) {
                    ResultHandle converterResolver = invoke.readInstanceField(resolverField, invoke.getThis());

                    ResultHandle result = invoke.invokeVirtualMethod(
                            MethodDescriptor.ofMethod(beanClassName, methodName,
                                    returnType,
                                    allParamTypes.toArray(new String[0])),
                            castBean, methodArgs);

                    BytecodeCreator replyBlock = invoke.ifNotNull(result).trueBranch();

                    ResultHandle replyPayload;
                    ResultHandle replyDest;

                    if (isJmsResponse) {
                        ResultHandle jmsResponse = replyBlock.checkCast(result, returnType);
                        replyPayload = replyBlock.invokeVirtualMethod(
                                MethodDescriptor.ofMethod(returnType, "getResponse", Object.class),
                                jmsResponse);

                        if (isSpringJmsResponse) {
                            replyDest = replyBlock.invokeStaticMethod(
                                    MethodDescriptor.ofMethod(
                                            "io.quarkiverse.spring.jms.runtime.SpringJmsResponseHelper",
                                            "resolveDestination",
                                            Destination.class,
                                            "org.springframework.jms.listener.adapter.JmsResponse",
                                            JMSContext.class),
                                    jmsResponse, jmsContext);
                        } else {
                            ResultHandle destName = replyBlock.invokeVirtualMethod(
                                    MethodDescriptor.ofMethod(returnType, "getDestination", String.class),
                                    jmsResponse);
                            ResultHandle isTopic = replyBlock.invokeVirtualMethod(
                                    MethodDescriptor.ofMethod(returnType, "isTopic", boolean.class),
                                    jmsResponse);

                            AssignableResultHandle destVar = replyBlock.createVariable(Destination.class);
                            BranchResult isTopicBranch = replyBlock.ifTrue(isTopic);
                            BytecodeCreator topicBranch = isTopicBranch.trueBranch();
                            topicBranch.assign(destVar,
                                    topicBranch.invokeInterfaceMethod(
                                            MethodDescriptor.ofMethod(JMSContext.class,
                                                    "createTopic", Topic.class, String.class),
                                            jmsContext, destName));
                            BytecodeCreator queueBranch = isTopicBranch.falseBranch();
                            queueBranch.assign(destVar,
                                    queueBranch.invokeInterfaceMethod(
                                            MethodDescriptor.ofMethod(JMSContext.class,
                                                    "createQueue", Queue.class, String.class),
                                            jmsContext, destName));
                            replyDest = destVar;
                        }
                    } else {
                        // @SendTo or JMSReplyTo
                        replyPayload = result;
                        if (listener.getSendToDestination() != null) {
                            if (listener.isSendToTopic()) {
                                replyDest = replyBlock.invokeInterfaceMethod(
                                        MethodDescriptor.ofMethod(JMSContext.class,
                                                "createTopic", Topic.class, String.class),
                                        jmsContext, replyBlock.load(listener.getSendToDestination()));
                            } else {
                                replyDest = replyBlock.invokeInterfaceMethod(
                                        MethodDescriptor.ofMethod(JMSContext.class,
                                                "createQueue", Queue.class, String.class),
                                        jmsContext, replyBlock.load(listener.getSendToDestination()));
                            }
                        } else {
                            replyDest = replyBlock.invokeInterfaceMethod(
                                    MethodDescriptor.ofMethod(Message.class, "getJMSReplyTo", Destination.class),
                                    message);
                        }
                    }

                    ResultHandle replyMessage = replyBlock.invokeVirtualMethod(
                            MethodDescriptor.ofMethod(JmsMessageConverterResolver.class,
                                    "toMessage", Message.class, Object.class, JMSContext.class),
                            converterResolver, replyPayload, jmsContext);

                    BytecodeCreator sendBlock = replyBlock.ifNotNull(replyDest).trueBranch();
                    ResultHandle producer = sendBlock.invokeInterfaceMethod(
                            MethodDescriptor.ofMethod(JMSContext.class, "createProducer", JMSProducer.class),
                            jmsContext);
                    sendBlock.invokeInterfaceMethod(
                            MethodDescriptor.ofMethod(JMSProducer.class, "send", JMSProducer.class,
                                    Destination.class, Message.class),
                            producer, replyDest, replyMessage);
                } else {
                    // Invoke void method
                    invoke.invokeVirtualMethod(
                            MethodDescriptor.ofMethod(beanClassName, methodName,
                                    void.class,
                                    allParamTypes.toArray(new String[0])),
                            castBean, methodArgs);
                }

                invoke.returnVoid();
            }
        }

        return invokerClassName;
    }

    private static ResultHandle resolveBean(MethodCreator mc, ResultHandle arc, String beanClassName) {
        ResultHandle beanClass = mc.loadClass(beanClassName);
        ResultHandle qualifiers = mc.newArray(Annotation.class, 0);
        ResultHandle handle = mc.invokeInterfaceMethod(
                MethodDescriptor.ofMethod("io.quarkus.arc.ArcContainer", "instance",
                        "io.quarkus.arc.InstanceHandle", Class.class, Annotation[].class),
                arc, beanClass, qualifiers);
        ResultHandle bean = mc.invokeInterfaceMethod(
                MethodDescriptor.ofMethod("io.quarkus.arc.InstanceHandle", "get", Object.class),
                handle);
        return mc.checkCast(bean, beanClassName);
    }

    private static ResultHandle resolveBean(MethodCreator mc, ResultHandle arc, Class<?> beanClass) {
        return resolveBean(mc, arc, beanClass.getName());
    }

    private static boolean hasCustomConverters(CombinedIndexBuildItem combinedIndex) {
        for (ClassInfo implementor : combinedIndex.getIndex().getAllKnownImplementations(JMS_CONVERTER)) {
            if (!implementor.name().equals(JACKSON_JMS_CONVERTER)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBuiltInType(String payloadType) {
        return switch (payloadType) {
            case "java.lang.String", "byte[]", "[B",
                    "java.util.Map", "java.util.HashMap",
                    "jakarta.jms.Message", "jakarta.jms.TextMessage",
                    "jakarta.jms.BytesMessage", "jakarta.jms.MapMessage",
                    "jakarta.jms.StreamMessage", "jakarta.jms.ObjectMessage" ->
                true;
            default -> false;
        };
    }

    private static boolean isJmsMessageType(String typeName) {
        return switch (typeName) {
            case "jakarta.jms.Message", "jakarta.jms.TextMessage",
                    "jakarta.jms.BytesMessage", "jakarta.jms.MapMessage",
                    "jakarta.jms.StreamMessage", "jakarta.jms.ObjectMessage" ->
                true;
            default -> false;
        };
    }

    private static ResultHandle generateHeaderExtraction(MethodCreator invoke, ResultHandle message,
            JmsListenerBuildItem.Parameter.Header header) {
        if (header.isAllHeaders()) {
            return invoke.invokeStaticMethod(
                    MethodDescriptor.ofMethod(
                            "io.quarkiverse.jms.runtime.JmsMessageHelper",
                            "extractProperties", java.util.Map.class, Message.class),
                    message);
        }
        ResultHandle headerName = invoke.load(header.headerName());
        return switch (header.type()) {
            case "int", "java.lang.Integer" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getIntProperty", int.class, String.class),
                    message, headerName);
            case "long", "java.lang.Long" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getLongProperty", long.class, String.class),
                    message, headerName);
            case "boolean", "java.lang.Boolean" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getBooleanProperty", boolean.class, String.class),
                    message, headerName);
            case "double", "java.lang.Double" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getDoubleProperty", double.class, String.class),
                    message, headerName);
            case "float", "java.lang.Float" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getFloatProperty", float.class, String.class),
                    message, headerName);
            case "short", "java.lang.Short" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getShortProperty", short.class, String.class),
                    message, headerName);
            case "byte", "java.lang.Byte" -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getByteProperty", byte.class, String.class),
                    message, headerName);
            default -> invoke.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(Message.class, "getStringProperty", String.class, String.class),
                    message, headerName);
        };
    }

    private static List<JmsListenerBuildItem.Parameter> classifyParameters(MethodInfo method) {
        boolean payloadFound = false;
        boolean rawMessageFound = false;
        List<JmsListenerBuildItem.Parameter> parameters = new ArrayList<>();

        for (int i = 0; i < method.parametersCount(); i++) {
            String typeName = method.parameterType(i).name().toString();

            AnnotationInstance headerAnn = findHeaderAnnotation(method, i);
            if (headerAnn != null) {
                String headerName = headerAnn.value() != null ? headerAnn.value().asString() : null;
                parameters.add(new JmsListenerBuildItem.Parameter.Header(typeName, headerName));
            } else if (!payloadFound && !isJmsMessageType(typeName)) {
                parameters.add(new JmsListenerBuildItem.Parameter.Payload(typeName));
                payloadFound = true;
            } else if (!payloadFound) {
                parameters.add(new JmsListenerBuildItem.Parameter.Payload(typeName));
                payloadFound = true;
            } else if (!rawMessageFound && isJmsMessageType(typeName)) {
                parameters.add(new JmsListenerBuildItem.Parameter.RawMessage(typeName));
                rawMessageFound = true;
            } else {
                throw new DeploymentException("@JmsListener method " + method.declaringClass().name() + "."
                        + method.name() + " has parameter '" + method.parameterName(i)
                        + "' (index " + i + ") that is neither the payload, a jakarta.jms.Message,"
                        + " nor annotated with @JmsHeader");
            }
        }
        return parameters;
    }

    private static AnnotationInstance findHeaderAnnotation(MethodInfo method, int paramIndex) {
        for (AnnotationInstance ann : method.annotations()) {
            if (ann.name().equals(DotNames.JMS_HEADER)
                    && ann.target().kind() == AnnotationTarget.Kind.METHOD_PARAMETER
                    && ann.target().asMethodParameter().position() == paramIndex) {
                return ann;
            }
        }
        return null;
    }

    private static String getStringValue(AnnotationInstance annotation, String name, String defaultValue) {
        AnnotationValue value = annotation.value(name);
        return value != null ? value.asString() : defaultValue;
    }

    private static boolean getBooleanValue(AnnotationInstance annotation, String name, boolean defaultValue) {
        AnnotationValue value = annotation.value(name);
        return value != null ? value.asBoolean() : defaultValue;
    }

    private static int getIntValue(AnnotationInstance annotation, String name, int defaultValue) {
        AnnotationValue value = annotation.value(name);
        return value != null ? value.asInt() : defaultValue;
    }
}
