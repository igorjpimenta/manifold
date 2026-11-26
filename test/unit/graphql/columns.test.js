
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { sqliteTable, text, integer, real } from "drizzle-orm/sqlite-core";
import {
  columnFields,
  objectType,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLID,
  GraphQLList,
  GraphQLNonNull,
} from "../../../graphql/columns.js";
import { flowRuns } from "../../../state/schema.js";

const sample = sqliteTable("sample_rows", {
  id: text("id").primaryKey(),
  display_name: text("display_name"),
  attempt_count: integer("attempt_count"),
  cost_usd: real("cost_usd"),
});

function typeName(field) {
  return field.type.toString();
}

describe("columnFields — scalar mapping from the drizzle def", () => {
  test("snake_case columns become camelCase fields with their column's scalar", () => {
    const fields = columnFields(sample, ["display_name", "attempt_count", "cost_usd"]);
    assert.deepEqual(Object.keys(fields), ["displayName", "attemptCount", "costUsd"]);
    assert.equal(typeName(fields.displayName), "String");
    assert.equal(typeName(fields.attemptCount), "Int");
    assert.equal(typeName(fields.costUsd), "Float");
  });

  test("the id column surfaces as ID! — never nullable", () => {
    const fields = columnFields(sample, ["id"]);
    assert.equal(fields.id.type instanceof GraphQLNonNull, true);
    assert.equal(fields.id.type.ofType, GraphQLID);
    assert.equal(typeName(fields.id), "ID!");
  });

  test("an unlisted column stays unpublished", () => {
    const fields = columnFields(sample, ["display_name"]);
    assert.deepEqual(Object.keys(fields), ["displayName"]);
  });
});

describe("columnFields — publication guard", () => {
  test("a column the table does not have throws, naming table and column", () => {
    assert.throws(
      () => columnFields(sample, ["no_such_column"]),
      /graphql: column no_such_column not on table sample_rows/,
    );
  });

  test("{ name, as } pins a stable API name while still requiring the db column", () => {
    const fields = columnFields(sample, [{ name: "display_name", as: "title" }]);
    assert.deepEqual(Object.keys(fields), ["title"]);
    assert.equal(typeName(fields.title), "String");
    assert.throws(
      () => columnFields(sample, [{ name: "gone", as: "x" }]),
      /column gone not on table/,
    );
  });
});

describe("columnFields — extras", () => {
  test("joined/derived extras are appended with their declared type", () => {
    const fields = columnFields(sample, ["id"], {
      pipelineId: GraphQLString,
      tags: new GraphQLList(GraphQLString),
      score: GraphQLFloat,
    });
    assert.equal(typeName(fields.pipelineId), "String");
    assert.equal(typeName(fields.tags), "[String]");
    assert.equal(typeName(fields.score), "Float");
  });

  test("an extra may override a column's published name", () => {
    const fields = columnFields(sample, ["display_name"], { displayName: GraphQLInt });
    assert.equal(typeName(fields.displayName), "Int");
  });

  test("no extras yields exactly the column map", () => {
    const fields = columnFields(sample, ["id", "cost_usd"]);
    assert.deepEqual(Object.keys(fields).sort(), ["costUsd", "id"]);
  });
});

describe("columnFields — real schema tables", () => {
  test("flow_runs derives fields from the live def — flow_id becomes flowId", () => {
    const fields = columnFields(flowRuns, ["flow_id", "status"]);
    assert.equal(typeName(fields.flowId), "String");
    assert.equal(typeName(fields.status), "String");
  });

  test("a column on the table is not published until it is listed", () => {
    const listed = columnFields(flowRuns, ["flow_id"]);
    assert.equal(listed.stateJson, undefined);
    assert.equal(listed.state_json, undefined);
    assert.equal(listed.ownerKey, undefined);
  });
});

describe("objectType — lazy field resolution", () => {
  test("builds an object type whose fields resolve to the same map", () => {
    const type = objectType({
      name: "SampleRow",
      table: sample,
      columns: ["id", "display_name"],
      extras: { pipelineId: GraphQLString },
      description: "one sample row",
    });
    assert.equal(type.name, "SampleRow");
    assert.equal(type.description, "one sample row");
    const fields = type.getFields();
    assert.deepEqual(Object.keys(fields), ["id", "displayName", "pipelineId"]);
    assert.equal(typeName(fields.id), "ID!");
  });

  test("fields resolve lazily — an invalid column only throws at first use, not at construction", () => {
    const type = objectType({ name: "Broken", table: sample, columns: ["missing"] });
    assert.throws(() => type.getFields(), /not on table/);
  });
});
