/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
class MapFromEntriesTest : public FunctionBaseTest {
 protected:
  void evaluateExprAndAssert(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<MapVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Evaluate an expression only, usually expect error thrown.
  void evaluateExpr(
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    evaluate(expression, makeRowVector(input));
  }

  void evaluateExprWithConstantArray(
      const std::vector<MapVectorPtr>& expectedVectors,
      const std::string& expression,
      const VectorPtr& input,
      const vector_size_t kConstantSize = 1'000) {
    auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
      return evaluate(
          expression,
          makeRowVector(
              {BaseVector::wrapInConstant(kConstantSize, row, vector)}));
    };
    for (auto row = 0; row < expectedVectors.size(); row++) {
      auto result = evaluateConstant(row, input);
      auto expectedConstant =
          BaseVector::wrapInConstant(kConstantSize, 0, expectedVectors[row]);
      assertEqualVectors(expectedConstant, result);
    }
  }
};
} // namespace

TEST_F(MapFromEntriesTest, intKeyAndIntValue) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, 11}), variant::row({2, 22}), variant::row({3, 33})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected =
      makeMapVector<int32_t, int32_t>({{{1, 11}, {2, 22}, {3, 33}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, intKeyAndVarcharValue) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, "red"}),
       variant::row({2, "blue"}),
       variant::row({3, "green"})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected = makeMapVector<int32_t, StringView>(
      {{{1, "red"_sv}, {2, "blue"_sv}, {3, "green"_sv}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, varcharKeyAndIntValue) {
  auto rowType = ROW({VARCHAR(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({"red shiny car ahead", 1}),
       variant::row({"blue clear sky above", 2}),
       variant::row({"yellow rose flowers", 3})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected = makeMapVector<StringView, int32_t>(
      {{{"red shiny car ahead"_sv, 1},
        {"blue clear sky above"_sv, 2},
        {"yellow rose flowers"_sv, 3}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, varcharKeyAndVarcharValue) {
  auto rowType = ROW({VARCHAR(), VARCHAR()});
  std::vector<std::vector<variant>> data = {
      {variant::row({"red", "red shiny car ahead"}),
       variant::row({"blue", "blue clear sky above"}),
       variant::row({"yellow", "yellow rose flowers"})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected = makeMapVector<StringView, StringView>(
      {{{"red"_sv, "red shiny car ahead"_sv},
        {"blue"_sv, "blue clear sky above"_sv},
        {"yellow"_sv, "yellow rose flowers"_sv}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, dateKeyAndBigintValue) {
  auto rowType = ROW({DATE(), BIGINT()});
  std::vector<std::vector<variant>> data = {
      {variant::row({Date(0), (int64_t)6945148781159939835}),
       variant::row({Date(10), (int64_t)6945148781159939835}),
       variant::row({Date(20), (int64_t)6945148781159939835})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected = makeMapVector<Date, int64_t>(
      {{{Date(0), 6945148781159939835},
        {Date(10), 6945148781159939835},
        {Date(20), 6945148781159939835}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, nullMapEntries) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant(TypeKind::ROW), variant::row({1, 11})}};
  auto input = makeArrayOfRowVector(rowType, data);
  VELOX_ASSERT_THROW(
      evaluateExpr("map_from_entries(C0)", {input}),
      "map entry cannot be null");
  EXPECT_NO_THROW(evaluateExpr("try(map_from_entries(C0))", {input}));
}

TEST_F(MapFromEntriesTest, nullKeys) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({variant::null(TypeKind::INTEGER), 0}),
       variant::row({1, 11})}};
  auto input = makeArrayOfRowVector(rowType, data);
  VELOX_ASSERT_THROW(
      evaluateExpr("map_from_entries(C0)", {input}), "map key cannot be null");
  EXPECT_NO_THROW(evaluateExpr("try(map_from_entries(C0))", {input}));
}

TEST_F(MapFromEntriesTest, duplicateKeys) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, 10}), variant::row({1, 11}), variant::row({2, 22})}};
  auto input = makeArrayOfRowVector(rowType, data);
  VELOX_ASSERT_THROW(
      evaluateExpr("map_from_entries(C0)", {input}),
      "Duplicate map keys (1) are not allowed");
  EXPECT_NO_THROW(evaluateExpr("try(map_from_entries(C0))", {input}));
}

TEST_F(MapFromEntriesTest, nullValues) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, variant::null(TypeKind::INTEGER)}),
       variant::row({2, 22}),
       variant::row({3, 33})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected =
      makeMapVector<int32_t, int32_t>({{{1, std::nullopt}, {2, 22}, {3, 33}}});
  evaluateExprAndAssert(expected, "map_from_entries(C0)", {input});
}

TEST_F(MapFromEntriesTest, moreThanTwoColumnsInRow) {
  auto rowType = ROW({INTEGER(), VARCHAR(), VARCHAR()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, "red shiny car ahead", "red"}),
       variant::row({2, "blue clear sky above", "blue"}),
       variant::row({3, "yellow rose flowers", "yellow"})}};
  auto input = makeArrayOfRowVector(rowType, data);
  EXPECT_THROW(evaluateExpr("map_from_entries(C0)", {input}), VeloxUserError);
}

TEST_F(MapFromEntriesTest, constantArray) {
  auto rowType = ROW({VARCHAR(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({"red", 1}),
       variant::row({"blue", 2}),
       variant::row({"green", 3})},
      {variant::row({"red shiny car ahead", 4}),
       variant::row({"blue clear sky above", 5})},
      {variant::row({"r", 11}),
       variant::row({"g", 22}),
       variant::row({"b", 33})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected = {
      makeMapVector<StringView, int32_t>(
          {{{"red"_sv, 1}, {"blue"_sv, 2}, {"green"_sv, 3}}}),
      makeMapVector<StringView, int32_t>(
          {{{"red shiny car ahead"_sv, 4}, {"blue clear sky above"_sv, 5}}}),
      makeMapVector<StringView, int32_t>(
          {{{"r"_sv, 11}, {"g"_sv, 22}, {"b"_sv, 33}}})};

  evaluateExprWithConstantArray(expected, "map_from_entries(C0)", input);
}
