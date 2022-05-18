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

package org.opensearch.querqy.rewriter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.querqy.ConfigUtils;
import org.opensearch.querqy.OpenSearchRewriterFactory;
import querqy.lucene.contrib.rewrite.wordbreak.MorphologicalWordBreaker;
import querqy.lucene.contrib.rewrite.wordbreak.Morphology;
import querqy.lucene.contrib.rewrite.wordbreak.SpellCheckerCompounder;
import querqy.lucene.contrib.rewrite.wordbreak.WordBreakCompoundRewriter;
import querqy.model.ExpandedQuery;
import querqy.model.Term;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;
import querqy.trie.TrieMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WordBreakCompoundRewriterFactory extends OpenSearchRewriterFactory {

    // this controls behaviour of the Lucene WordBreakSpellChecker:
    // for compounds: maximum distance of leftmost and rightmost term index
    //                e.g. max_changes = 1 for A B C D will check AB BC CD,
    //                     max_changes = 2 for A B C D will check AB ABC BC BCD CD
    // for decompounds: maximum splits performed
    //                  e.g. max_changes = 1 for ABCD will check A BCD, AB CD, ABC D,
    //                       max_changes = 2 for ABCD will check A BCD, A B CD, A BC D, AB CD, AB C D, ABC D
    // as we currently only send 2-grams to WBSP for compounding only max_changes = 1 is correctly supported
    static final int MAX_CHANGES = 1;

    static final int MAX_EVALUATIONS = 100;

    static final int DEFAULT_MIN_SUGGESTION_FREQ = 1;
    static final int DEFAULT_MAX_COMBINE_LENGTH = 30;
    static final int DEFAULT_MIN_BREAK_LENGTH = 3;
    static final int DEFAULT_MAX_DECOMPOUND_EXPANSIONS = 3;
    static final boolean DEFAULT_LOWER_CASE_INPUT = false;
    static final boolean DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS = false;
    static final boolean DEFAULT_VERIFY_DECOMPOUND_COLLATION = false;


    private String dictionaryField;

    private boolean lowerCaseInput = DEFAULT_LOWER_CASE_INPUT;
    private boolean alwaysAddReverseCompounds = DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS;

    private WordBreakSpellChecker spellChecker;
    private SpellCheckerCompounder compounder;
    private MorphologicalWordBreaker wordBreaker;
    private TrieMap<Boolean> reverseCompoundTriggerWords;
    private TrieMap<Boolean> protectedWords;
    private int maxDecompoundExpansions = DEFAULT_MAX_DECOMPOUND_EXPANSIONS;
    private boolean verifyDecompoundCollation = DEFAULT_VERIFY_DECOMPOUND_COLLATION;


    public WordBreakCompoundRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(final Map<String, Object> config) {

        final int minSuggestionFreq = ConfigUtils.getArg(config, "minSuggestionFreq", DEFAULT_MIN_SUGGESTION_FREQ);
        final int maxCombineLength = ConfigUtils.getArg(config, "maxCombineLength", DEFAULT_MAX_COMBINE_LENGTH);
        final int minBreakLength = ConfigUtils.getArg(config, "minBreakLength", DEFAULT_MIN_BREAK_LENGTH);
        dictionaryField = ConfigUtils.getStringArg(config, "dictionaryField")
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing config:  dictionaryField"));
        lowerCaseInput = ConfigUtils.getArg(config, "lowerCaseInput", DEFAULT_LOWER_CASE_INPUT);
        alwaysAddReverseCompounds = ConfigUtils.getArg(config, "alwaysAddReverseCompounds",
                DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);

        spellChecker = new WordBreakSpellChecker();
        spellChecker.setMaxChanges(MAX_CHANGES);
        spellChecker.setMinSuggestionFrequency(minSuggestionFreq);
        spellChecker.setMaxCombineWordLength(maxCombineLength);
        spellChecker.setMinBreakWordLength(minBreakLength);
        spellChecker.setMaxEvaluations(100);
        compounder = new SpellCheckerCompounder(spellChecker, dictionaryField, lowerCaseInput);

        final Morphology morphology = ConfigUtils.getEnumArg(config, "morphology", Morphology.class)
                .orElse(Morphology.DEFAULT);

        wordBreaker = new MorphologicalWordBreaker(morphology, dictionaryField, lowerCaseInput, minSuggestionFreq,
                minBreakLength, MAX_EVALUATIONS);

        reverseCompoundTriggerWords = ConfigUtils.getTrieSetArg(config, "reverseCompoundTriggerWords");
        protectedWords = ConfigUtils.getTrieSetArg(config, "protectedWords");

        Map<String, Object> decompoundConf = (Map<String, Object>) config.get("decompound");
        if (decompoundConf == null) {
            decompoundConf = Collections.emptyMap();
        }
        maxDecompoundExpansions = ConfigUtils.getArg(decompoundConf, "maxExpansions",
                DEFAULT_MAX_DECOMPOUND_EXPANSIONS);
        verifyDecompoundCollation =  ConfigUtils.getArg(decompoundConf, "verifyCollation",
                DEFAULT_VERIFY_DECOMPOUND_COLLATION);

    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {

        final List<String> errors = new LinkedList<>();
        final Optional<String> optValue = ConfigUtils.getStringArg(config, "dictionaryField").map(String::trim)
                .filter(s -> !s.isEmpty());
        if (!optValue.isPresent()) {
            errors.add("Missing config:  dictionaryField");
        }

        ConfigUtils.getStringArg(config, "morphology").ifPresent(morphologyName -> {
            if (Arrays.stream(Morphology.values()).map(Enum::name).noneMatch(name -> name.equals(morphologyName))) {
                errors.add("Unknown morphology: " + morphologyName);
            }
        });

        return errors;

    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) {

        return new RewriterFactory(getRewriterId()) {
            @Override
            public QueryRewriter createRewriter(final ExpandedQuery input,
                                                final SearchEngineRequestAdapter searchEngineRequestAdapter) {


                return new WordBreakCompoundRewriter(wordBreaker, compounder, getShardIndexReader(indexShard),
                        lowerCaseInput, alwaysAddReverseCompounds, reverseCompoundTriggerWords, maxDecompoundExpansions,
                        verifyDecompoundCollation, protectedWords);


            }

            @Override
            public Set<Term> getGenerableTerms() {
                return QueryRewriter.EMPTY_GENERABLE_TERMS;
            }
        };
    }

    public WordBreakSpellChecker getSpellChecker() {
        return spellChecker;
    }

    public String getDictionaryField() {
        return dictionaryField;
    }


    public boolean isLowerCaseInput() {
        return lowerCaseInput;
    }

    public boolean isAlwaysAddReverseCompounds() {
        return alwaysAddReverseCompounds;
    }

    public TrieMap<Boolean> getReverseCompoundTriggerWords() {
        return reverseCompoundTriggerWords;
    }

    public TrieMap<Boolean> getProtectedWords() {
        return protectedWords;
    }

    public int getMaxDecompoundExpansions() {
        return maxDecompoundExpansions;
    }

    public boolean isVerifyDecompoundCollation() {
        return verifyDecompoundCollation;
    }

    private IndexReader getShardIndexReader(final IndexShard indexShard) {

        try (Engine.Searcher searcher = indexShard.acquireSearcher("WordBreakCompoundRewriter")) {
            return searcher.getTopReaderContext().reader();
        }
    }

    public SpellCheckerCompounder getCompounder() {
        return compounder;
    }

    public MorphologicalWordBreaker getWordBreaker() {
        return wordBreaker;
    }


}
