import _ from "lodash";
import { API_CONFIG } from "../../config/api";
import {
  Constraint,
  DataType,
  EntitySchema,
  EntitySchemaAttribute,
  EntitySchemaVersion,
  Node,
  Tag,
} from "./catalog.slice";

export const fetchNodes = async (): Promise<Node[]> => {
  let nodes = [] as Node[];
  let response = await fetch(`${API_CONFIG.origin}/api/v1/catalog`);
  let catalog = await response.json();

  let groupedByNode = _.groupBy(catalog.entities, getNodeUrl);

  for (let nodeUrl in groupedByNode) {
    let nodeInfo = await fetchNodeInfo(nodeUrl);
    let rawEntities = groupedByNode[nodeUrl];

    let entityVersions: EntitySchemaVersion[] = rawEntities.map(
      (e: any) =>
        ({
          id: `ev-${e.id}`,
          version: e.source.table_name.split("$")[1] || "latest",
          tableName: e.source.table_name,
          attributes: e.attributes.map((a: any) => convertAttribute(e, a)),
          constraints: e.constraints.map((c: any, idx: number) =>
            convertConstraint(e, c, idx)
          ),
          e,
        } as EntitySchemaVersion)
    );

    let entities = _.toPairs(
      _.groupBy(entityVersions, (e) => e.tableName.split("$")[0])
    ).map(([tablePrefix, versions]) => {
      let latest = findLatestVersion(versions);
      return {
        id: `e-${tablePrefix}`,
        name: (latest as any).e.name || tablePrefix,
        description: (latest as any).e.description,
        tags: (latest as any).e.tags,
        versions,
      } as EntitySchema;
    });

    let allTags = _.uniq(entities.flatMap((e) => e.tags.map((t) => t.key))).map(
      (tag) => ({ id: `t-${tag}`, name: tag, description: "" } as Tag)
    );

    nodes.push({
      id: `n-${nodeUrl}`,
      name: nodeInfo?.name || getNodeNameFallback(nodeUrl),
      url: nodeUrl,
      tags: allTags,
      schema: {
        entities,
      },
      jobs: [],
    });
  }

  return nodes;
};

export const isAuthoritative = (node: Node) =>
  node.url.startsWith(window.location.origin);

const getNodeUrl = (entity: any): string => {
  if (!entity.source.url) {
    return API_CONFIG.origin;
  }

  let source = entity.source;

  while (source.source && source.source.url) {
    source = source.source;
  }

  return source.url;
};

const getNodeNameFallback = (url: string): string => {
  try {
    return new URL(url).hostname;
  } catch (e) {
    console.warn(`Error while parsing url`, url, e);
  }

  return url;
};

const fetchNodeInfo = async (url: string): Promise<any> => {
  try {
    let res = await fetch(`${url}/api/v1/node`);
    return await res.json();
  } catch (e) {
    console.warn(`Failed to fetch node info from ${url}, perhaps it is down?`);

    return null;
  }
};

const TYPE_NAME_MAP = {
  Utf8String: "TEXT",
  DateTimeWithTz: "TIMESTAMP WITH TIME ZONE",
} as { [i: string]: string };

const convertType = (type: any): DataType => {
  if (typeof type === "object") {
    const typeName: string = Object.keys(type)[0] || "Unknown";
    let opts = [];

    for (let optName in type[typeName] || {}) {
      opts.push(`${type[typeName][optName]}`);
    }

    let name = TYPE_NAME_MAP[typeName] || typeName.toUpperCase();

    return { name: opts.length ? `${name}(${opts.join(", ")})` : name };
  } else {
    return { name: String(type).toUpperCase() };
  }
};

const convertAttribute = (e: any, a: any): EntitySchemaAttribute => {
  return {
    id: `eva-${e.id}.${a.id}`,
    name: a.id,
    description: a.description || "",
    type: convertType(a.type),
    primaryKey: a.primary_key,
    validations: a.nullable ? [] : [{ name: "NOT NULL" }],
  };
};

const convertConstraint = (e: any, c: any, idx: number): Constraint => {
  if (c.type === "foreign_key") {
    return {
      id: `cst-${e.id}-${idx}`,
      type: "fk",
      attributes: Object.keys(c.attribute_map).map(
        (a: string) => `eva-${e.id}.${a}`
      ),
      targetEntity: `e-${c.target_entity_id}`,
      targetAttributes: (Object.values(c.attribute_map) as string[]).map(
        (a: string) => `eva-${c.target_entity_id}.${a}`
      ),
    };
  }

  if (c.type === "unique") {
    return {
      id: `cst-${e.id}-${idx}`,
      type: "unique",
      attributes: c.attributes.map((a: string) => `eva-${e.id}-a`),
    };
  }

  return {
    id: `cst-${e.id}-${idx}`,
    type: "unknown",
    attributes: [],
  };
};

const findLatestVersion = (
  versions: EntitySchemaVersion[]
): EntitySchemaVersion => {
  return (
    versions.find((v) => v.version === "latest") ||
    _.sortBy(versions, (v) => v.version).reverse()[0]
  );
};
