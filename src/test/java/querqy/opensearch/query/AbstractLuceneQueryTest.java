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

package querqy.opensearch.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import querqy.lucene.rewrite.DependentTermQueryBuilder;
import querqy.lucene.rewrite.FieldBoost;
import querqy.lucene.rewrite.IndependentFieldBoost;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public  class AbstractLuceneQueryTest {

   public static ClauseMatcher tq(Occur occur, float boost, String field, String text) {
      return c(occur, tq(boost, field, text));
   }

   public static ClauseMatcher tq(Occur occur, String field, String text) {
      return c(occur, tq(field, text));
   }

   public static TQMatcher tq(float boost, String field, String text) {
      return new TQMatcher(boost, field, text);
   }

   public static TQMatcher tq(String field, String text) {
      return tq(1f, field, text);
   }
   
   public static ClauseMatcher dtq(Occur occur, float boost, String field, String text) {
       return c(occur, dtq(boost, field, text));
   }

   public static ClauseMatcher dtq(Occur occur, String field, String text) {
       return c(occur, dtq(field, text));
   }
    

   public static DependentTQMatcher dtq(float boost, String field, String text) {
       return new DependentTQMatcher(boost, field, text);
   }
   
   public static DependentTQMatcher dtq(String field, String text) {
       return dtq(1f, field, text);
   }
   
   public static BQMatcher bq(float boost, int mm, ClauseMatcher... clauses) {
      return new BQMatcher(boost, mm, clauses);
   }

   public static BQMatcher bq(float boost, ClauseMatcher... clauses) {
      return new BQMatcher(boost, 0, clauses);
   }

   public static BQMatcher bq(ClauseMatcher... clauses) {
      return bq(1f, clauses);
   }
   
   public static ClauseMatcher all(Occur occur) {
       return c(occur, all());
   }
   
   public static AllDocsQueryMatcher all() {return  new AllDocsQueryMatcher(); }

   public static ClauseMatcher bq(Occur occur, float boost, int mm, ClauseMatcher... clauses) {
      return c(occur, bq(boost, mm, clauses));
   }

   public static ClauseMatcher bq(Occur occur, float boost, ClauseMatcher... clauses) {
      return c(occur, bq(boost, clauses));
   }

   public static ClauseMatcher bq(Occur occur, ClauseMatcher... clauses) {
      return c(occur, bq(clauses));
   }

   public static DMQMatcher dmq(Float boost, Float tieBreaker, List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return new DMQMatcher(boost, tieBreaker, disjuncts);
   }

   public static DMQMatcher dmq(Float boost, List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return dmq(boost, null, disjuncts);
   }

   public static  DMQMatcher dmq(List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return dmq((Float) null, null, disjuncts);
   }

   public static  ClauseMatcher dmq(Occur occur, Float boost, Float tieBreaker,
                                    List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return c(occur, dmq(boost, tieBreaker, disjuncts));
   }

   public static  ClauseMatcher dmq(Occur occur, Float boost, List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return c(occur, dmq(boost, disjuncts));
   }

   public static  ClauseMatcher dmq(Occur occur, List<TypeSafeMatcher<? extends Query>> disjuncts) {
      return c(occur, dmq(disjuncts));
   }

   public static ClauseMatcher c(Occur occur, TypeSafeMatcher<? extends Query> queryMatcher) {
      return new ClauseMatcher(occur, queryMatcher);
   }

    public static OccurClauseMatcher anyMust() {
       return new OccurClauseMatcher(Occur.MUST);
    }

    public static OccurClauseMatcher anyShould() {
        return new OccurClauseMatcher(Occur.SHOULD);
    }

    public static OccurClauseMatcher anyMustNot() {
        return new OccurClauseMatcher(Occur.MUST_NOT);
    }

    public static OccurClauseMatcher anyFilter() {
        return new OccurClauseMatcher(Occur.FILTER);
    }

    static class OccurClauseMatcher extends TypeSafeMatcher<BooleanClause> {
        Occur occur;

        OccurClauseMatcher(final Occur occur) {
            this.occur = occur;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("occur: " + occur);
        }

        @Override
        protected boolean matchesSafely(final BooleanClause clause) {
            return clause.getOccur() == occur;
        }
    }

    public static class ClauseMatcher extends TypeSafeMatcher<BooleanClause> {

      Occur occur;
      TypeSafeMatcher<? extends Query> queryMatcher;

      public ClauseMatcher(Occur occur, TypeSafeMatcher<? extends Query> queryMatcher) {
         this.occur = occur;
         this.queryMatcher = queryMatcher;
      }

      @Override
      public void describeTo(Description description) {
         description.appendText("occur: " + occur);
         queryMatcher.describeTo(description);
      }

      @Override
      protected boolean matchesSafely(BooleanClause clause) {
         return clause.getOccur() == occur && queryMatcher.matches(clause.getQuery());
      }

    }

    static class DMQMatcher extends TypeSafeMatcher<Query> {
        final  Float boost;
        final Float tieBreaker;
        final List<TypeSafeMatcher<? extends Query>> disjuncts;

        DMQMatcher(final Float boost, final Float tieBreaker, List<TypeSafeMatcher<? extends Query>> disjuncts) {
            super((boost == null || boost == 1f)
                    ? DisjunctionMaxQuery.class : BoostQuery.class);
            this.boost = boost;
            this.tieBreaker = tieBreaker;
            this.disjuncts = disjuncts;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("DMQ: ");
            if (tieBreaker != null) {
                description.appendText("tieBreaker=").appendValue(tieBreaker);
            }
            if (boost != null) {
                description.appendText("boost=").appendValue(boost);
            }
            description.appendList("disjuncts:[", ",\n", "]", disjuncts);
        }

        @Override
        protected boolean matchesSafely(Query query) {

            DisjunctionMaxQuery dmq;

            if (query instanceof BoostQuery) {
                BoostQuery boostQuery = (BoostQuery) query;
                if (boostQuery.getBoost() != boost) {
                    return false;
                } else {
                    dmq = (DisjunctionMaxQuery) boostQuery.getQuery();
                }
            } else {
                if (boost != null && boost != 1f) {
                    return false;
                } else {
                    dmq = (DisjunctionMaxQuery) query;
                }
            }

            return matchDisjunctionMaxQuery(dmq);

        }

        protected boolean matchDisjunctionMaxQuery(DisjunctionMaxQuery dmq) {

            if (tieBreaker != null && tieBreaker != dmq.getTieBreakerMultiplier()) {
                return false;
            }

            Collection<Query> dmqDisjuncts = dmq.getDisjuncts();
            if (dmqDisjuncts.size() != disjuncts.size()) {
                return false;
            }

            for (TypeSafeMatcher<? extends Query> disjunct : disjuncts) {
                boolean found = false;
                for (Query q : dmqDisjuncts) {
                    found = disjunct.matches(q);
                    if (found) {
                        break;
                    }
                }
                if (!found) {
                    return false;
                }

            }
            return true;
        }
    }

    static class AllDocsQueryMatcher extends TypeSafeMatcher<Query> {

        @Override
        public void describeTo(Description description) {
            description.appendText("AllDocs");
        }

        @Override
        protected boolean matchesSafely(Query item) {
            return MatchAllDocsQuery.class.isAssignableFrom(item.getClass());
        }
       
    }

    static class BQMatcher extends TypeSafeMatcher<Query> {

        ClauseMatcher[] clauses;
        int mm;
        float boost;

        BQMatcher(float boost, int mm, ClauseMatcher... clauses) {
            super((boost == 1f) ? BooleanQuery.class : BoostQuery.class);
            this.clauses = clauses;
            this.boost = boost;
            this.mm = mm;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("BQ: mm=" + mm + ", boost=" + boost + ", ");
            description.appendList("clauses:[", ",\n", "]", Arrays.asList(clauses));
        }

        @Override
        protected boolean matchesSafely(Query query) {

            BooleanQuery bq = null;

            if (query instanceof BoostQuery) {

                Query boostedQuery = ((BoostQuery) query).getQuery();
                if (!(boostedQuery instanceof BooleanQuery)) {
                    return false;
                }

                if (((BoostQuery) query).getBoost() != boost) {
                    return false;
                }

                bq = (BooleanQuery) boostedQuery;

            } else if (!(query instanceof BooleanQuery)) {
                return false;
            } else {
                if (boost != 1f) {
                    return false;
                }
                bq = (BooleanQuery) query;
            }

            return matchBooleanQuery(bq);

        }

        protected boolean matchBooleanQuery(BooleanQuery bq) {

            if (mm != bq.getMinimumNumberShouldMatch()) {
                return false;
            }

            List<BooleanClause> bqClauses = bq.clauses();
            if (bqClauses == null || bqClauses.size() != clauses.length) {
                return false;
            }

            for (int i = 0; i < clauses.length; i++) {

                boolean found = false;
                for (BooleanClause clause : bqClauses) {
                    found = clauses[i].matches(clause);
                    if (found) {
                        break;
                    }
                }

                if (!found) {
                    return false;
                }

            }
            return true;
        }

    }



    static class DependentTQMatcher extends TypeSafeMatcher<Query> {

       final String field;
       final String text;
       final float boost;

       DependentTQMatcher(float boost, String field, String text) {
          super(DependentTermQueryBuilder.DependentTermQuery.class);
          this.field = field;
          this.text = text;
          this.boost = boost;
       }

       @Override
       public void describeTo(Description description) {
          description.appendText("DTQ field: " + field + ", text: " + text + ", boost: " + boost);

       }

       @Override
       protected boolean matchesSafely(Query termQuery) {
          Term term = ((DependentTermQueryBuilder.DependentTermQuery) termQuery).getTerm();
          if (!field.equals(term.field()) || !text.equals(term.text())) {
              return false;
          }
          FieldBoost fieldBoost = ((DependentTermQueryBuilder.DependentTermQuery) termQuery).getFieldBoost();
          if (fieldBoost == null) {
              return false;
          }
          if (fieldBoost != null && fieldBoost instanceof IndependentFieldBoost) {
              return boost == ((IndependentFieldBoost) fieldBoost).getBoost(term.field());
          } else {
              throw new RuntimeException("Cannot test FieldBoosts other than IndependentFieldBoost");
          }
                
       }

    }


    static class TQMatcher extends TypeSafeMatcher<Query> {

      final String field;
      final String text;
      final float boost;

      TQMatcher(float boost, String field, String text) {
         super(TermQuery.class);
         this.field = field;
         this.text = text;
         this.boost = boost;
      }

      @Override
      public void describeTo(Description description) {
         description.appendText("TQ field: " + field + ", text: " + text + ", boost: " + boost);

      }

      @Override
      protected boolean matchesSafely(Query query) {

          if (query instanceof BoostQuery) {

              BoostQuery boostQuery = (BoostQuery) query;

              Query boostedQuery = boostQuery.getQuery();
              if (boostedQuery instanceof TermQuery) {
                  Term term = ((TermQuery) query).getTerm();
                  return field.equals(term.field())
                          && text.equals(term.text())
                          && boostQuery.getBoost() == boost;
              } else {
                  return false;
              }
          } else if (query instanceof TermQuery) {
              Term term = ((TermQuery) query).getTerm();
              return field.equals(term.field())
                      && text.equals(term.text())
                      && boost == 1f;
          } else {
              return false;
          }

      }

   }

}
