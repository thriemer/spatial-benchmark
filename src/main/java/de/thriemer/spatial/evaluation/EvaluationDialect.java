package de.thriemer.spatial.evaluation;


import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.query.sqm.function.FunctionKind;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.query.sqm.produce.function.PatternFunctionDescriptorBuilder;
import org.hibernate.query.sqm.produce.function.StandardFunctionReturnTypeResolvers;
import org.hibernate.type.spi.TypeConfiguration;

public class EvaluationDialect extends H2Dialect {


    @Override
    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);
        SqmFunctionRegistry registry = functionContributions.getFunctionRegistry();
        TypeConfiguration types = functionContributions.getTypeConfiguration();

        new PatternFunctionDescriptorBuilder(registry, "harmonic_avg", FunctionKind.AGGREGATE, "HARMONIC_AVG(?1)")
                .setExactArgumentCount(1)
                .setInvariantType(types.getBasicTypeForJavaType(Double.class))
                .register();
        new PatternFunctionDescriptorBuilder(registry, "HARMONIC_ERR_PROP", FunctionKind.AGGREGATE, "HARMONIC_ERR_PROP(?1, ?2)")
                .setExactArgumentCount(2)
                .setInvariantType(types.getBasicTypeForJavaType(Double.class))
                .register();
        new PatternFunctionDescriptorBuilder(registry, "GEOMETRIC_AVG", FunctionKind.AGGREGATE, "GEOMETRIC_AVG(?1)")
                .setExactArgumentCount(1)
                .setInvariantType(types.getBasicTypeForJavaType(Double.class))
                .register();
        new PatternFunctionDescriptorBuilder(registry, "GEOMETRIC_ERR_PROP", FunctionKind.AGGREGATE, "GEOMETRIC_ERR_PROP(?1, ?2)")
                .setExactArgumentCount(2)
                .setInvariantType(types.getBasicTypeForJavaType(Double.class))
                .register();
        new PatternFunctionDescriptorBuilder(registry, "SCENARIO_MATCHES", FunctionKind.NORMAL, "SCENARIO_MATCHES(?1, ?2)")
                .setExactArgumentCount(2)
                .setInvariantType(types.getBasicTypeForJavaType(String.class))
                .setReturnTypeResolver(StandardFunctionReturnTypeResolvers.invariant(types.getBasicTypeForJavaType(Boolean.class)))
                .register();
        new PatternFunctionDescriptorBuilder(registry, "EXTRACT_SYSTEM_METRIC_NAME", FunctionKind.NORMAL, "EXTRACT_SYSTEM_METRIC_NAME(?1)")
                .setExactArgumentCount(1)
                .setInvariantType(types.getBasicTypeForJavaType(String.class))
                .register();


    }

}