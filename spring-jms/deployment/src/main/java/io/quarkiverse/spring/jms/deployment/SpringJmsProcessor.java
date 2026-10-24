package io.quarkiverse.spring.jms.deployment;

import static io.quarkus.deployment.annotations.ExecutionTime.RUNTIME_INIT;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.inject.spi.DeploymentException;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationTransformation;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.logging.Logger;

import io.quarkiverse.jms.deployment.JmsListenerBuildItem;
import io.quarkiverse.spring.jms.runtime.SpringJmsClientAdapter;
import io.quarkiverse.spring.jms.runtime.SpringJmsRecorder;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.AnnotationsTransformerBuildItem;
import io.quarkus.arc.deployment.CustomScopeAnnotationsBuildItem;
import io.quarkus.arc.deployment.SyntheticBeansRuntimeInitBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class SpringJmsProcessor {

    private static final Logger LOG = Logger.getLogger(SpringJmsProcessor.class);

    private static final DotName SPRING_JMS_LISTENER = DotName.createSimple(
            "org.springframework.jms.annotation.JmsListener");
    private static final DotName SPRING_JMS_LISTENERS = DotName.createSimple(
            "org.springframework.jms.annotation.JmsListeners");
    private static final DotName SPRING_SEND_TO = DotName.createSimple(
            "org.springframework.messaging.handler.annotation.SendTo");
    private static final DotName SPRING_HEADER = DotName.createSimple(
            "org.springframework.messaging.handler.annotation.Header");
    private static final DotName SPRING_HEADERS = DotName.createSimple(
            "org.springframework.messaging.handler.annotation.Headers");
    private static final DotName SPRING_PAYLOAD = DotName.createSimple(
            "org.springframework.messaging.handler.annotation.Payload");

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("spring-jms");
    }

    @BuildStep
    AnnotationsTransformerBuildItem transformBeanScope(CustomScopeAnnotationsBuildItem scopes) {
        return new AnnotationsTransformerBuildItem(AnnotationTransformation.forClasses()
                .transform(ctx -> {
                    ClassInfo clazz = ctx.declaration().asClass();
                    if (scopes.isScopeIn(ctx.annotations())) {
                        return;
                    }
                    if (clazz.annotationsMap().containsKey(SPRING_JMS_LISTENER)
                            || clazz.annotationsMap().containsKey(SPRING_JMS_LISTENERS)) {
                        ctx.add(AnnotationInstance.builder(
                                DotName.createSimple("jakarta.enterprise.context.ApplicationScoped")).build());
                    }
                }));
    }

    @BuildStep
    void registerBeans(BuildProducer<AdditionalBeanBuildItem> beans) {
        beans.produce(AdditionalBeanBuildItem.builder()
                .addBeanClass(SpringJmsClientAdapter.class)
                .setUnremovable()
                .build());
    }

    @BuildStep
    @Record(RUNTIME_INIT)
    @Consume(SyntheticBeansRuntimeInitBuildItem.class)
    void configurePubSubDomain(SpringJmsBuildTimeConfig config, SpringJmsRecorder recorder) {
        recorder.configurePubSubDomain(config.pubSubDomain());
    }

    @BuildStep
    void mapSpringListeners(CombinedIndexBuildItem combinedIndex, SpringJmsBuildTimeConfig springConfig,
            BuildProducer<JmsListenerBuildItem> listeners) {

        List<AnnotationInstance> allAnnotations = new ArrayList<>(
                combinedIndex.getIndex().getAnnotations(SPRING_JMS_LISTENER));

        for (AnnotationInstance container : combinedIndex.getIndex().getAnnotations(SPRING_JMS_LISTENERS)) {
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
            ClassInfo declaringClass = method.declaringClass();

            if (method.parametersCount() == 0) {
                throw new DeploymentException("@JmsListener method " + declaringClass.name() + "." +
                        method.name() + " must have at least one parameter");
            }

            String destination = annotation.value("destination").asString();
            String selector = getStringValue(annotation, "selector", "");
            String subscription = getStringValue(annotation, "subscription", "");
            String concurrencyStr = getStringValue(annotation, "concurrency", "");
            String containerFactory = getStringValue(annotation, "containerFactory", "");

            boolean topic = springConfig.pubSubDomain();
            String acknowledgeMode = "";
            int concurrency = 0;
            int maxConcurrency = -1;

            if (!containerFactory.isEmpty()) {
                var factoryConfig = springConfig.containerFactories().get(containerFactory);
                if (factoryConfig != null) {
                    topic = factoryConfig.pubSubDomain();
                    acknowledgeMode = factoryConfig.acknowledgeMode().orElse("");
                    String factoryConcurrency = factoryConfig.concurrency().orElse("");
                    if (!factoryConcurrency.isEmpty() && concurrencyStr.isEmpty()) {
                        concurrencyStr = factoryConcurrency;
                    }
                }
            }

            if (!concurrencyStr.isEmpty()) {
                if (concurrencyStr.contains("-")) {
                    String[] parts = concurrencyStr.split("-");
                    concurrency = Integer.parseInt(parts[0].trim());
                    maxConcurrency = Integer.parseInt(parts[1].trim());
                } else {
                    concurrency = Integer.parseInt(concurrencyStr.trim());
                }
            }

            String payloadParameterType = null;
            List<JmsListenerBuildItem.Parameter> parameters = new ArrayList<>();

            for (int i = 0; i < method.parametersCount(); i++) {
                String typeName = method.parameterType(i).name().toString();
                boolean isHeader = false;
                for (AnnotationInstance ann : method.annotations()) {
                    if (ann.target().kind() == AnnotationTarget.Kind.METHOD_PARAMETER
                            && ann.target().asMethodParameter().position() == i) {
                        if (ann.name().equals(SPRING_HEADER)) {
                            String headerName = getStringValue(ann, "value", "");
                            if (headerName.isEmpty()) {
                                headerName = getStringValue(ann, "name", "");
                            }
                            parameters.add(new JmsListenerBuildItem.Parameter.Header(
                                    typeName, headerName));
                            isHeader = true;
                            break;
                        } else if (ann.name().equals(SPRING_HEADERS)) {
                            parameters.add(new JmsListenerBuildItem.Parameter.Header(
                                    typeName, null));
                            isHeader = true;
                            break;
                        }
                    }
                }
                if (!isHeader) {
                    boolean isPayload = false;
                    for (AnnotationInstance ann : method.annotations()) {
                        if (ann.name().equals(SPRING_PAYLOAD)
                                && ann.target().kind() == AnnotationTarget.Kind.METHOD_PARAMETER
                                && ann.target().asMethodParameter().position() == i) {
                            isPayload = true;
                            break;
                        }
                    }
                    if (isPayload || payloadParameterType == null) {
                        payloadParameterType = typeName;
                        parameters.add(new JmsListenerBuildItem.Parameter.Payload(typeName));
                    } else {
                        parameters.add(new JmsListenerBuildItem.Parameter.Payload(typeName));
                    }
                }
            }
            if (payloadParameterType == null) {
                payloadParameterType = method.parameterType(0).name().toString();
            }

            AnnotationInstance sendToAnn = method.annotation(SPRING_SEND_TO);
            String sendToDestination = null;
            if (sendToAnn != null && sendToAnn.value() != null) {
                String[] values = sendToAnn.value().asStringArray();
                if (values.length > 0 && !values[0].isEmpty()) {
                    sendToDestination = values[0];
                }
            }

            listeners.produce(new JmsListenerBuildItem(
                    method,
                    parameters,
                    payloadParameterType,
                    destination,
                    topic,
                    selector,
                    "",
                    concurrency,
                    maxConcurrency,
                    subscription,
                    acknowledgeMode,
                    false,
                    sendToDestination,
                    false));

            LOG.infof("Mapped Spring @JmsListener: %s.%s -> %s",
                    declaringClass.simpleName(), method.name(), destination);
        }
    }

    private static String getStringValue(AnnotationInstance annotation, String name, String defaultValue) {
        AnnotationValue value = annotation.value(name);
        return value != null ? value.asString() : defaultValue;
    }
}
