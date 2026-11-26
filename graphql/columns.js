// graphql/columns.js — derive GraphQL leaf fields from the drizzle table
// defs, so the query layer inherits state/schema*.js (what drizzle-kit
// generates migrations from) instead of restating it. Exposure is explicit:
// each type lists its columns, so a new column is one word to publish and
// internals (state_json, tokens, handles) never leak by default; a test
// asserts the built SDL, so a renamed column breaks loudly.
import {
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLID,
  GraphQLList,
  GraphQLNonNull,
} from "graphql";
import { getTableConfig } from "drizzle-orm/sqlite-core";

const SCALARS = {
  SQLiteText: GraphQLString,
  SQLiteInteger: GraphQLInt,
  SQLiteReal: GraphQLFloat,
};

function camel(name) {
  return name.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

/**
 * `columns` entries are db snake_case names, or { name, as } to pin an API
 * name (the db name must still exist, so renames break loudly). Extras are
 * joined/derived values declared with explicit types. Only `id` is non-null:
 * LEFT JOINs and derivation make DB nullability a liar for the rest.
 */
export function columnFields(table, columns, extras = {}) {
  const byName = {};
  for (const col of getTableConfig(table).columns) byName[col.name] = col;
  const fields = {};
  for (const entry of columns) {
    const name = typeof entry === "string" ? entry : entry.name;
    const gqlName = typeof entry === "string" ? camel(entry) : entry.as;
    const col = byName[name];
    if (!col) throw new Error(`graphql: column ${name} not on table ${getTableConfig(table).name}`);
    const scalar = SCALARS[col.columnType] ?? GraphQLString;
    fields[gqlName] = { type: name === "id" ? new GraphQLNonNull(GraphQLID) : scalar };
  }
  for (const [gqlName, type] of Object.entries(extras)) fields[gqlName] = { type };
  return fields;
}

export function objectType({ name, table, columns, extras, description }) {
  return new GraphQLObjectType({
    name,
    description,
    fields: () => columnFields(table, columns, extras),
  });
}

export { GraphQLString, GraphQLInt, GraphQLFloat, GraphQLID, GraphQLList, GraphQLNonNull };
