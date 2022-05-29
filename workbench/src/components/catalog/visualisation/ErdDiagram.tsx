import Box from "@mui/material/Box";
import * as d3 from "d3";
import _ from "lodash";
import { useEffect, useRef, useState } from "react";
import { EntitySchema, EntitySchemaVersion, Id, Node } from "../catalog.slice";

interface Props {
  nodes: Node[];
}

type Entity = EntitySchema & EntitySchemaVersion & { node: Node };
type RelationshipType = "1" | "0..1" | "n";
interface Relationship {
  source: Id;
  target: Id;
  sourceType: RelationshipType;
  targetType: RelationshipType;
}

export const ErdDiagram = (props: Props) => {
  const container = useRef<HTMLDivElement>();

  // @ts-ignore
  useEffect(() => {
    // delay for container width to calculate
    setTimeout(() => {
      if (!container.current) {
        return;
      }

      const [entities, relationships] = constructGraphData(props.nodes);

      const chart = D3ErdGraph(entities, relationships, (e) => e.node.id, {
        width: container.current.clientWidth,
        height: container.current.clientHeight,
      });

      for (const child of Array.from(container.current.childNodes)) {
        child.remove();
      }
      container.current?.appendChild(chart);
    }, 0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.nodes.length]);

  return (
    <Box
      sx={{ width: "100%", height: "100%", overflow: "hidden" }}
      ref={container}
    ></Box>
  );
};

const constructGraphData = (nodes: Node[]): [Entity[], Relationship[]] => {
  const entities: Entity[] = nodes.flatMap((n) =>
    n.schema.entities.map((e) => ({
      ...e,
      ...e.versions[e.versions.length - 1],
      node: n,
      id: e.id,
    }))
  );

  const entityLookup = _.keyBy(entities, (e) => e.id);

  const relationships: Relationship[] = entities.flatMap(
    (e) =>
      e.constraints
        ?.filter((c) => c.type === "fk")
        .map(
          (c) =>
            ({
              source: e.id,
              target: c.targetEntity!,
              sourceType: getRelationshipType(e, c.attributes),
              targetType: getRelationshipType(
                entityLookup[c.targetEntity!],
                c.targetAttributes!
              ),
            } as Relationship)
        ) || []
  );

  return [entities, relationships];
};

const getRelationshipType = (e: Entity, attrIds: Id[]): RelationshipType => {
  // TODO: this should derive from unique constraints, not nulls etc
  const attrs = attrIds.map((id) => e.attributes.find((a) => a.id === id));
  if (attrs.some((a) => a?.name === "id") && attrs.length === 1) {
    return "1";
  } else {
    return "n";
  }
};

interface EntityNode extends d3.SimulationNodeDatum {
  id: string;
  entity: Entity;
  open: boolean;
  width: number;
  height: number;
}

// @ts-ignore
const D3ErdGraph = (
  entities: Entity[],
  relationships: Relationship[],
  groupingFn: (e: Entity) => any,
  {
    linkStroke = "#999", // link stroke color
    linkStrokeOpacity = 0.6, // link stroke opacity
    linkStrokeWidth = 1.5, // given d in links, returns a stroke width in pixels
    linkStrokeLinecap = "round", // link stroke linecap
    colors = d3.schemeDark2, // an array of color strings, for the node groups
    width = 640, // outer width, in pixels
    height = 400, // outer height, in pixels
  } = {}
) => {
  // Compute grouping
  const GROUP = d3.map(entities, groupingFn).map(intern);
  const nodeGroups = d3.sort(GROUP);
  const color = d3.scaleOrdinal(nodeGroups, colors);

  // Compute values.
  const EID = d3.map(entities, (e) => e.id).map(intern);
  const RSRC = d3.map(relationships, (r) => r.source).map(intern);
  const RTGT = d3.map(relationships, (r) => r.target).map(intern);
  const ENTITY_HTML_DOCS = d3.map(entities, (e) =>
    renderEntityHtml(e, color(e.node.id))
  );

  // Replace the input nodes and links with mutable objects for the simulation.
  const nodes = d3
    .map(
      entities,
      (e, i) =>
        ({
          id: EID[i],
          entity: e,
          open: true,
          width: 0,
          height: 0,
        } as EntityNode)
    )
    .map((d) => ({ ...d, ...calculateEntityDimensions(d) }));
  const links = d3.map(relationships, (_, i) => ({
    source: RSRC[i],
    target: RTGT[i],
  }));

  // Construct the forces.
  const forceNode = d3.forceManyBody().strength(500);
  const forceLink = d3
    .forceLink(links)
    .id(({ index: i }) => EID[i!])
    .distance(300)
    .strength(1);

  const forceGrouping = d3
    .forceLink(
      entities.flatMap((e) =>
        entities
          .filter((e2) => groupingFn(e) === groupingFn(e2) && e !== e2)
          .map((e2) => ({
            source: e.id,
            target: e2.id,
          }))
      )
    )
    .id(({ index: i }) => EID[i!])
    .strength(0.01);

  // TODO: right angle lines
  // TODO: unique starting position by category

  const simulation = d3
    .forceSimulation(nodes)
    .force("link", forceLink)
    .force("grouping", forceGrouping)
    .force("charge", forceNode)
    .force("collide", d3.forceCollide(150))
    .force("center", d3.forceCenter(0, 0))
    .on("tick", ticked);

  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [-width / 2, -height / 2, width, height])
    .attr("style", "max-width: 100%; height: auto; height: intrinsic;");

  const g = svg.append("g");

  const link = g
    .append("g")
    .attr("stroke", linkStroke)
    .attr("stroke-opacity", linkStrokeOpacity)
    .attr("stroke-width", 3)
    .attr("stroke-linecap", linkStrokeLinecap)
    .selectAll("line")
    .data(links)
    .join("line");

  const node = g
    .append("g")
    .selectAll("svg")
    .data(nodes)
    .join("svg")
    .call(drag(simulation) as any);

  node
    .append("foreignObject")
    .attr("width", (d) => d.width)
    .attr("height", (d) => d.height)
    .append("xhtml:div")
    .style("font", "14px 'Helvetica Neue'")
    .html(({ index: i }) => `${ENTITY_HTML_DOCS[i!]}`);

  node.attr("fill", ({ index: i }) => color(GROUP[i!]));

  function intern(value: any) {
    return value !== null && typeof value === "object"
      ? value.valueOf()
      : value;
  }

  const clipX = (x: number, d: EntityNode) => {
    return x
    // return Math.max(Math.min(x, width / 2 - d.width), -width / 2);
  };
  const clipY = (y: number, d: any) => {
    return y
    // return Math.max(Math.min(y, 3 * height / 2 - d.height), -height / 2);
  };

  function ticked() {
    link
      .attr("x1", (d) => d.source.x + d.source.width / 2)
      .attr("y1", (d) => d.source.y + d.source.height / 2)
      .attr("x2", (d) => d.target.x + d.target.width / 2)
      .attr("y2", (d) => d.target.y + d.target.height / 2);

    node.attr("x", (d) => clipX(d.x!, d)).attr("y", (d) => clipY(d.y!, d));
  }

  function drag(simulation: d3.Simulation<any, any>) {
    function dragstarted(event: d3.D3DragEvent<any, any, any>) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      event.subject.fx = event.subject.x;
      event.subject.fy = event.subject.y;
    }

    function dragged(event: d3.D3DragEvent<any, any, any>) {
      event.subject.fx = clipX(event.x, event.subject);
      event.subject.fy = clipY(event.y, event.subject);
    }

    function dragended(event: d3.D3DragEvent<any, any, any>) {
      if (!event.active) simulation.alphaTarget(0);
      event.subject.fx = null;
      event.subject.fy = null;
    }

    return d3
      .drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended);
  }

  // handle opening/closing entities
  node.on("click", function (e: MouseEvent) {
    if (e.defaultPrevented) return; // dragged

    const node = d3.select(this);
    const d = node.datum() as EntityNode;

    d.open = !d.open;
    const newDims = calculateEntityDimensions(d);
    d.width = newDims.width;
    d.height = newDims.height;
    node
      .select("foreignObject")
      .attr("width", d.width)
      .attr("height", d.height);
  });

  // zooming
  const zoom = d3.zoom();
  svg.call(zoom as any);
  zoom.on("zoom", (e: d3.D3ZoomEvent<any, any>) => {
    g.attr("transform", e.transform as any);
  });

  return Object.assign(svg.node(), { scales: { color } });
};

const renderEntityHtml = (e: Entity, color: string): string => {
  const id = e.id.replace(/[^a-zA-Z0-9]/g, "_");
  const dims = calculateEntityDimensions();
  const col1Width = dims.width * 0.6;
  const col2Width = dims.width * 0.4;

  return `<html>
    <head>
      <style>

      #${id}-erd {
        font-family: "Roboto","Helvetica","Arial",sans-serif;
        width: ${dims.width}px;
        background: none;
        color: #eee;
        overflow: hidden;
      }

      #${id}-erd h1 {
        margin: 0;
        font-size: 20px;
        text-align: center;
        font-weight: normal;
        border: 1px solid #ccc;
        cursor: pointer;
        width: 100%;
        padding: 4px;
        background: ${color};
      }

      #${id}-erd table {
        background: ${color};
        border-collapse: collapse;
        table-layout: fixed;
      }

      #${id}-erd table tr > * {
        border: 1px solid #ccc;
        padding: 2px;
        font-weight: normal;
        word-wrap: break-word;
        overflow: hidden;
      }

      #${id}-erd table tr > *:first-child {
        text-align: left;
        width: ${col1Width}px;
      }

      #${id}-erd table tr > *:last-child {
        width: ${col2Width}px;
        text-align: right;
      }
      </style>
    </head>
    <div id="${id}-erd">
      <h1>${e.name}</h1>
      <table cellspacing="0">
        <tbody>
        ${e.attributes
          .map(
            (a) => `<tr>
          <td style="font-size: ${Math.min(
            14,
            (1.5 * col1Width) / a.name.length
          )}px">${a.name}</td>
          <td style="font-size: ${Math.min(
            14,
            (1.5 * col2Width) / a.type.name.length
          )}px">${a.type.name}</td>
        </tr>`
          )
          .join("")}
        </tbody>
      </table>
    </div>
  </html>`;
};

const calculateEntityDimensions = (d?: EntityNode) => {
  return {
    width: 200,
    height: 34 + (d?.open ? d.entity.attributes.length * 22.5 : 0),
  };
};
