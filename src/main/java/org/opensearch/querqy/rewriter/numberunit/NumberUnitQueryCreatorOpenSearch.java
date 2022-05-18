/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.querqy.rewriter.numberunit;

import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.WeightBuilder;
import org.opensearch.querqy.query.QueryBuilderRawQuery;
import querqy.model.BoostQuery;
import querqy.model.Clause;
import querqy.model.RawQuery;
import querqy.rewrite.contrib.numberunit.NumberUnitQueryCreator;
import querqy.rewrite.contrib.numberunit.model.NumberUnitDefinition;
import querqy.rewrite.contrib.numberunit.model.PerUnitNumberUnitDefinition;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class NumberUnitQueryCreatorOpenSearch extends NumberUnitQueryCreator {

    public NumberUnitQueryCreatorOpenSearch(int scale) {
        super(scale);
    }

    protected RawQuery createRawBoostQuery(final BigDecimal value,
                                           final List<PerUnitNumberUnitDefinition> perUnitNumberUnitDefinitions) {

        final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        final BoolQueryBuilder boolQueryBuilderLowerFilter = new BoolQueryBuilder();
        final BoolQueryBuilder boolQueryBuilderUpperFilter = new BoolQueryBuilder();

        final List<FilterFunctionBuilder> filterFunctionBuildersLower = new ArrayList<>();
        final List<FilterFunctionBuilder> filterFunctionBuildersExact = new ArrayList<>();
        final List<FilterFunctionBuilder> filterFunctionBuildersUpper = new ArrayList<>();

        perUnitNumberUnitDefinitions.forEach(perUnitDef -> {
            final NumberUnitDefinition numberUnitDef = perUnitDef.numberUnitDefinition;

            final BigDecimal standardizedValue = value.multiply(perUnitDef.multiplier);

            final BigDecimal lowerBound = subtractPercentage(standardizedValue,
                    numberUnitDef.boostPercentageLowerBoundary);

            final BigDecimal lowerBoundExactMatch = subtractPercentage(standardizedValue,
                    numberUnitDef.boostPercentageLowerBoundaryExactMatch);

            final BigDecimal upperBound = addPercentage(standardizedValue,
                    numberUnitDef.boostPercentageUpperBoundary);

            final BigDecimal upperBoundExactMatch = addPercentage(standardizedValue,
                    numberUnitDef.boostPercentageUpperBoundaryExactMatch);

            final BigDecimal lowerOrigin = standardizedValue.subtract(standardizedValue.subtract(lowerBoundExactMatch));

            final BigDecimal lowerScale = lowerBoundExactMatch.subtract(lowerBound)
                    .divide(BigDecimal.valueOf(2), super.getRoundingMode());

            final BigDecimal lowerDecay = calculateDecay(numberUnitDef.maxScoreForExactMatch,
                    numberUnitDef.minScoreAtLowerBoundary);

            final BigDecimal upperOrigin = standardizedValue.add(upperBoundExactMatch.subtract(standardizedValue));

            final BigDecimal upperScale = upperBound.subtract(upperBoundExactMatch)
                    .divide(BigDecimal.valueOf(2), super.getRoundingMode());

            final BigDecimal upperDecay = calculateDecay(numberUnitDef.maxScoreForExactMatch,
                    numberUnitDef.minScoreAtUpperBoundary);

            perUnitDef.numberUnitDefinition.fields.forEach(field -> {
                boolQueryBuilderLowerFilter.should(
                        new RangeQueryBuilder(field.fieldName)
                                .gte(lowerBound.setScale(field.scale, super.getRoundingMode()).doubleValue())
                                .lt(lowerBoundExactMatch.setScale(field.scale, super.getRoundingMode()).doubleValue()));

                boolQueryBuilderUpperFilter.should(
                        new RangeQueryBuilder(field.fieldName)
                                .gt(upperBoundExactMatch.setScale(field.scale, super.getRoundingMode()).doubleValue())
                                .lte(upperBound.setScale(field.scale, super.getRoundingMode()).doubleValue()));

                filterFunctionBuildersLower.add(
                        new FilterFunctionBuilder(
                                new LinearDecayFunctionBuilder(
                                        field.fieldName,
                                        lowerOrigin.setScale(field.scale, super.getRoundingMode()).doubleValue(),
                                        lowerScale.doubleValue(),
                                        0,
                                        lowerDecay.doubleValue())
                                        .setWeight(numberUnitDef.maxScoreForExactMatch.floatValue())));

                filterFunctionBuildersExact.add(
                        new FilterFunctionBuilder(
                                new RangeQueryBuilder(field.fieldName)
                                        .gte(lowerBoundExactMatch.setScale(field.scale, super.getRoundingMode()).doubleValue())
                                        .lte(upperBoundExactMatch.setScale(field.scale, super.getRoundingMode()).doubleValue()),
                                new WeightBuilder()
                                        .setWeight(numberUnitDef.maxScoreForExactMatch
                                                .add(numberUnitDef.additionalScoreForExactMatch).floatValue())));

                filterFunctionBuildersUpper.add(
                        new FilterFunctionBuilder(
                                new LinearDecayFunctionBuilder(
                                        field.fieldName,
                                        upperOrigin.setScale(field.scale, super.getRoundingMode()).doubleValue(),
                                        upperScale.doubleValue(),
                                        0,
                                        upperDecay.doubleValue())
                                        .setWeight(numberUnitDef.maxScoreForExactMatch.floatValue())));
            });
        });

        boolQueryBuilder
                .should(new FunctionScoreQueryBuilder(boolQueryBuilderLowerFilter,
                        filterFunctionBuildersLower.toArray(new FilterFunctionBuilder[0]))
                        .boostMode(CombineFunction.MULTIPLY)
                        .scoreMode(FunctionScoreQuery.ScoreMode.MAX))
                .should(new FunctionScoreQueryBuilder(filterFunctionBuildersExact.toArray(new FilterFunctionBuilder[0])))
                .should(new FunctionScoreQueryBuilder(boolQueryBuilderUpperFilter,
                        filterFunctionBuildersUpper.toArray(new FilterFunctionBuilder[0]))
                        .boostMode(CombineFunction.MULTIPLY)
                        .scoreMode(FunctionScoreQuery.ScoreMode.MAX));

        return new QueryBuilderRawQuery(null, boolQueryBuilder, Clause.Occur.MUST, true);
    }

    @Override
    public BoostQuery createBoostQuery(final BigDecimal value,
                                       final List<PerUnitNumberUnitDefinition> perUnitNumberUnitDefinitions) {
        return new BoostQuery(createRawBoostQuery(value, perUnitNumberUnitDefinitions), 1.0f);
    }

    @Override
    public RawQuery createFilterQuery(final BigDecimal value,
                                      final List<PerUnitNumberUnitDefinition> perUnitNumberUnitDefinitions) {

        final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.minimumShouldMatch(1);

        perUnitNumberUnitDefinitions.forEach(def -> {
            final BigDecimal multipliedValue = value.multiply(def.multiplier);

            final BigDecimal lowerBound = def.numberUnitDefinition.filterPercentageLowerBoundary.compareTo(BigDecimal.ZERO) >= 0
                    ? subtractPercentage(multipliedValue, def.numberUnitDefinition.filterPercentageLowerBoundary)
                    : def.numberUnitDefinition.filterPercentageLowerBoundary;

            final BigDecimal upperBound = def.numberUnitDefinition.filterPercentageUpperBoundary.compareTo(BigDecimal.ZERO) >= 0
                    ? addPercentage(multipliedValue, def.numberUnitDefinition.filterPercentageUpperBoundary)
                    : def.numberUnitDefinition.filterPercentageUpperBoundary;

            def.numberUnitDefinition.fields.forEach(field -> {
                RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(field.fieldName);
                if (lowerBound.compareTo(BigDecimal.ZERO) >= 0) {
                    rangeQueryBuilder.gte(lowerBound.setScale(field.scale, super.getRoundingMode()).doubleValue());
                }

                if (upperBound.compareTo(BigDecimal.ZERO) >= 0) {
                    rangeQueryBuilder.lte(upperBound.setScale(field.scale, super.getRoundingMode()).doubleValue());
                }

                boolQueryBuilder.should(rangeQueryBuilder);
            });

        });

        return new QueryBuilderRawQuery(null, boolQueryBuilder, Clause.Occur.SHOULD, true);
    }

    private BigDecimal calculateDecay(BigDecimal maxValue, BigDecimal minValue) {
        final BigDecimal decayGround = minValue.divide(maxValue, super.getRoundingMode());
        final BigDecimal decaySummand = BigDecimal.ONE.subtract(decayGround)
                .divide(BigDecimal.valueOf(2), super.getRoundingMode());

        return decayGround.add(decaySummand);

    }

}
