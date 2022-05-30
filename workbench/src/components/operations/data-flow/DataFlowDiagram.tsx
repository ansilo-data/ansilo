import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import { darken } from "@mui/material/styles";
import Typography from "@mui/material/Typography";
import DataFlowIcon from "@mui/icons-material/Air";
import * as d3 from "d3";
import _ from "lodash";
import { useEffect, useRef, useState } from "react";
import { Node } from "../../catalog/catalog.slice";

interface Props {
  nodes: Node[];
}

interface Flow {
  source: string;
  target: string;
  records: number;
  failures: number;
}

export const DataFlowDiagram = (props: Props) => {
  const container = useRef<HTMLDivElement>();
  const [graph, setGraph] = useState<ReturnType<typeof D3DataFlowGraph>>();

  useEffect(() => {
    // delay for container width to calculate
    setTimeout(() => {
      if (!container.current) {
        return;
      }

      const [nodes, flows] = constructGraphData(props.nodes);

      const graph = D3DataFlowGraph(nodes, flows, {
        width: container.current.clientWidth,
        height: container.current.clientHeight,
      });

      for (const child of Array.from(container.current.childNodes)) {
        child.remove();
      }
      container.current?.appendChild(graph);
      setGraph(graph);
    }, 0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.nodes.length]);

  return (
    <Box
      sx={{
        width: "100%",
        height: "100%",
        position: "relative",
        p: 4,
      }}
    >
      <Typography
        variant="h4"
        sx={{
          position: "absolute",
          left: 32,
          top: 32,
          display: "flex",
          alignItems: "center",
          zIndex: 1,
          background: "#",
        }}
      >
        <DataFlowIcon fontSize="large" sx={{ mr: 1 }} /> Data Flow
      </Typography>
      <Button
        sx={{ position: "absolute", right: 32, top: 32, zIndex: 1 }}
        onClick={() => graph?.callbacks.zoomToFitAll()}
        variant="contained"
        color="secondary"
      >
        Reset Zoom
      </Button>
      <Box
        sx={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          overflow: "hidden",
          opacity: graph ? 1 : 0,
        }}
        ref={container}
      ></Box>
    </Box>
  );
};

const constructGraphData = (nodes: Node[]): [Node[], Flow[]] => {
  // TODO: use proper node SQL name
  const flows: Flow[] = nodes.flatMap(
    (n) =>
      n.jobs?.flatMap((j) =>
        nodes
          .filter((n2) =>
            j.query.sql.includes(
              " " + n2.name.replace(/ /g, "_").toLowerCase() + "."
            )
          )
          .map(
            (n2) =>
              ({
                source: n2.id,
                target: n.id,
                records:
                  _.sum(j.runs.map((r) => r.recordsCount || 0)) /
                  j.runs.filter((r) => r.status === "success").length,
                failures: j.runs.filter((r) => r.status === "error").length,
              } as Flow)
          )
      ) || []
  );

  return [nodes, flows];
};

interface GraphNode extends d3.SimulationNodeDatum {
  id: string;
  node: Node;
}

const D3DataFlowGraph = (
  nodeData: Node[],
  flowData: Flow[],
  {
    colors = d3.schemeDark2, // an array of color strings, for the node groups
    width = 640, // outer width, in pixels
    height = 400, // outer height, in pixels
  } = {}
) => {
  // Compute values.
  const EID = d3.map(nodeData, (e) => e.id).map(intern);
  const color = d3.scaleOrdinal(EID, colors);
  const NODE_HTML_DOCS = d3.map(nodeData, (e) =>
    renderNodeHtml(e, color(e.id))
  );

  // Replace the input nodes and links with mutable objects for the simulation.
  const nodes = d3
    .map(
      nodeData,
      (n, i) =>
        ({
          id: EID[i],
          node: n,
        } as GraphNode)
    )
    .map((d) => ({ ...d, ...calculateNodeDimensions(d) }));
  const links = d3.map(flowData, (r, i) => ({
    source: nodes.find(n => n.id === r.source),
    target: nodes.find(n => n.id === r.target),
    strength: r.records,
    failure: r.failures > 0,
  }));
  console.log(links)

  // Construct the forces.
  const forceNode = d3.forceManyBody().strength(20);
  const forceLink = d3
    .forceLink(links)
    .id(({ index: i }) => EID[i!])
    .strength(1)
    .distance(300)
    // .strength((l) => l.strength);

  const simulation = d3
    .forceSimulation(nodes)
    // .force("link", forceLink)
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
    .attr("stroke-linecap", "round")
    .attr("fill", "none")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke", (l) => (l.failure ? "#ff9999" : "#999"))
    .attr("stroke-width", (l) => l.strength / 100);

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
    .html(({ index: i }) => `${NODE_HTML_DOCS[i!]}`);

  node.attr("fill", ({ id }) => color(id));

  function intern(value: any) {
    return value !== null && typeof value === "object"
      ? value.valueOf()
      : value;
  }

  function ticked() {
    link
      .attr("x1", (l) => l.source.x!)
      .attr("y1", (l) => l.source.y!)
      .attr("x2", (l) => l.target.x!)
      .attr("y2", (l) => l.target.y!);

    node.attr("x", (d) => Math.round(d.x!)).attr("y", (d) => Math.round(d.y!));
  }

  function drag(simulation: d3.Simulation<any, any>) {
    function dragstarted(event: d3.D3DragEvent<any, any, any>) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      event.subject.fx = event.subject.x;
      event.subject.fy = event.subject.y;
    }

    function dragged(event: d3.D3DragEvent<any, any, any>) {
      event.subject.fx = event.x;
      event.subject.fy = event.y;
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

  // zooming
  const zoom = d3.zoom();
  svg.call(zoom as any).on("dblclick.zoom", null);

  zoom.on("zoom", (e: d3.D3ZoomEvent<any, any>) => {
    if (e.sourceEvent?.type !== "mousemove") {
      g.transition()
        .duration(200)
        .delay(0)
        .attr("transform", e.transform as any);
    } else {
      g.attr("transform", e.transform as any);
    }
  });

  const zoomToFitAll = () => {
    const padding = 50;
    const xmin = Math.min(...nodes.map((i) => i.x!)) - padding;
    const ymin = Math.min(...nodes.map((i) => i.y!)) - padding;
    const xmax = Math.max(...nodes.map((i) => i.x! + i.width)) + padding;
    const ymax = Math.max(...nodes.map((i) => i.y! + i.height)) + padding;

    const dx = xmax - xmin;
    const dy = ymax - ymin;

    const zx = width / dx;
    const zy = height / dy;

    const cx = (xmax + xmin) / 2;
    const cy = (ymax + ymin) / 2;

    svg.call(
      zoom.transform as any,
      d3.zoomIdentity.scale(Math.min(zx, zy)).translate(-cx, -cy)
    );
  };

  // // run simulation until stable
  // while (true) {
  //   for (let i = 0; i <= 50; i++) {
  //     simulation.tick();
  //   }

  //   const vmax = Math.max(
  //     ...nodes.map((i) => i.vx!),
  //     ...nodes.map((i) => i.vy!)
  //   );

  //   if (vmax < 5) {
  //     break;
  //   }
  // }

  // initial zoom
  zoomToFitAll();

  return Object.assign(svg.node(), {
    scales: { color },
    callbacks: { zoomToFitAll },
  });
};

const renderNodeHtml = (n: Node, color: string): string => {
  const id = n.id.replace(/[^a-zA-Z0-9]/g, "_");
  const dims = calculateNodeDimensions();

  return `<html>
    <head>
      <style>

      #${id}-node {
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
        background: ${darken(color, 0.2)};
      }
      </style>
    </head>
    <div id="${id}-node">
      <h1>${n.name}</h1>
    </div>
  </html>`;
};

const calculateNodeDimensions = (d?: GraphNode) => {
  return {
    width: 200,
    height: 50,
  };
};
