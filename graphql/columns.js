// graphql/columns.js — derive GraphQL leaf fields from the drizzle
// table defs, so the query layer inherits the database schema instead
// of restating it.
//
// Single source of truth stays state/schema*.js (the same files
// drizzle-kit generates migrations from). What is PUBLIC is still an
// explicit choice: each GraphQL type lists the columns it exposes, so a
// new column is one word to publish and never leaks silently —
// internals like state_json or tokens stay out by default. A test
// asserts the built SDL, so a renamed column breaks loudly instead of
// drifting. Joined/derived extras (pipeline_id via flow_runs, joined
// node ids) are declared with explicit types beside the table list.
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
 * Build the fields map for one GraphQL object type from a drizzle table.
 * columns: exposed entries — a db snake_case name, or { name, as } to
 * pin a stable API name that differs from the column (renames still
 * break loudly: the db name must exist). Extras: { gqlName: type } for
 * joined/derived values that travel beside the row. `id` columns
 * surface as ID!; everything else stays nullable because LEFT JOINs and
 * derivation make DB nullability a liar.
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
