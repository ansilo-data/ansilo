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
  id: string;
  source: string;
  target: string;
  throughput: number;
  runs: number;
  failures: number;
  lastFailureMessage?: string;
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

      if (graph) {
        graph.callbacks.dispose();
      }

      const [nodes, flows] = constructGraphData(props.nodes);

      const newGraph = D3DataFlowGraph(nodes, flows, {
        width: container.current.clientWidth,
        height: container.current.clientHeight,
      });

      for (const child of Array.from(container.current.childNodes)) {
        child.remove();
      }
      container.current?.appendChild(newGraph);
      setGraph(newGraph);
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
  let id = 0;

  const flows: Flow[] = nodes.flatMap((n) =>
    _.values(
      (
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
                  id: `f-${id++}`,
                  source: n2.id,
                  target: n.id,
                  throughput:
                    _.sum(j.runs.map((r) => r.recordsCount || 0)) /
                    j.runs.filter((r) => r.status === "success").length,
                  runs: j.runs.length,
                  failures: j.runs.filter((r) => r.status === "error").length,
                  lastFailureMessage: j.runs.find(r => r.status === "error" && r.message)?.message,
                } as Flow)
            )
        ) || []
      )
        // group by [source, target]
        .reduce((a, i) => {
          const key = `${i.source}-${i.target}`;
          if (a[key]) {
            a[key].throughput += i.throughput;
            a[key].failures += i.failures;
            a[key].lastFailureMessage = a[key].lastFailureMessage || i.lastFailureMessage;
          } else {
            a[key] = i;
          }
          return a;
        }, {} as { [k: string]: Flow })
    )
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

  const throughputDomain = [
    d3.min(flowData.map((i) => i.throughput))!,
    d3.max(flowData.map((i) => i.throughput))!,
  ];
  const linkStrength = d3.scaleLinear(throughputDomain, [0.03, 0.1]);
  const linkWidth = d3.scaleLinear(throughputDomain, [2, 8]);

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
    source: r.source as any,
    target: r.target as any,
    flow: r,
    strength: linkStrength(r.throughput),
    width: linkWidth(r.throughput),
    failure: r.failures > 0,
  }));

  // Construct the forces.
  const forceNode = d3.forceManyBody().strength(20);
  const forceLink = d3
    .forceLink(links)
    .id(({ index: i }) => EID[i!])
    .strength(1)
    .distance(200)
    .strength((l) => l.strength);

  const simulation = d3
    .forceSimulation(nodes)
    .force("link", forceLink)
    .force("charge", forceNode)
    .force("collide", d3.forceCollide(100))
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
    .style("pointer-events", "none")
    .attr("fill", "none")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke-linecap", "round")
    .attr("stroke", (l) => (l.failure ? "#aa5555" : "#555"))
    .attr("stroke-width", (l) => l.width)
    .attr("stroke-dasharray", (l) => `${l.width * 0} ${l.width * 2}`);

  const linkHoverLines = g
    .append("g")
    .attr("fill", "none")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke", (l) => (l.failure ? "#aa5555" : "#555"))
    .style("z-index", 10)
    .style("opacity", "0")
    .attr("stroke-width", (l) => l.width * 2);

  // flow animation
  const animateFlow = () => {
    link
      .transition()
      .duration(5000)
      .ease(d3.easeLinear)
      .attrTween("stroke-dashoffset", ((l: any) =>
        d3.interpolate(l.width * 2 * 3, 0)) as any)
      .on("end", animateFlow);
  };
  animateFlow();

  // tooltips
  const tooltips = d3
    .select("body")
    .append("div")
    .attr("id", "data-flow-tooltips")
    .selectAll("div")
    .data(links)
    .join("div")
    .style("position", "fixed")
    .attr("id", (f) => f.flow.id)
    .style("font", "14px 'Helvetica Neue'")
    .html((f) => renderTooltip(f.flow));

  linkHoverLines.each((f, i, lines) => {
    d3.selectAll([lines[i]])
      .on(
        "mouseenter",
        _.debounce((e: MouseEvent) => {
          const el = document.getElementById(f.flow.id);
          if (el) {
            el.style.display = "block";
          }
        }, 1)
      )
      .on(
        "mouseleave",
        _.debounce((e: MouseEvent) => {
          const f = d3.select(e.target as any).datum() as any;
          const el = document.getElementById(f.flow.id);
          console.log(el)
          if (el) {
            el.style.display = "none";
          }
        }, 1000)
      )
      .on("mousemove", (e: MouseEvent) => {
        const f = d3.select(e.target as any).datum() as any;
        const el = document.getElementById(f.flow.id);
        if (el) {
          el.style.left = e.pageX + "px";
          el.style.top = e.pageY + "px";
        }
      });
  });

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
    for (const links of [link, linkHoverLines]) {
      links
        .attr("x1", (l) => Math.round(l.source.x! + l.source.width! / 2))
        .attr("y1", (l) => Math.round(l.source.y! + l.source.height! / 2))
        .attr("x2", (l) => Math.round(l.target.x! + l.target.width! / 2))
        .attr("y2", (l) => Math.round(l.target.y! + l.target.height! / 2));
    }

    node
      .attr("x", (d) => (d.x || 0).toFixed(1))
      .attr("y", (d) => (d.y || 0).toFixed(1));
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

  // run simulation until stable
  while (true) {
    for (let i = 0; i <= 50; i++) {
      simulation.tick();
    }

    const vmax = Math.max(
      ...nodes.map((i) => i.vx!),
      ...nodes.map((i) => i.vy!)
    );

    if (vmax < 5) {
      break;
    }
  }

  // initial zoom
  zoomToFitAll();

  const dispose = () => {
    document.getElementById("data-flow-tooltips")?.remove();
  };

  return Object.assign(svg.node(), {
    scales: { color },
    callbacks: { zoomToFitAll, dispose },
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
        height: ${dims.height}px;
        background: #222;
        border: 2px solid #1f1f1f;
        border-radius: 400px;
        padding: 18px;
        overflow: hidden;
        cursor: pointer;
        position: relative;
        display: flex;
        justify-content: center;
      }

      #${id}-node h1 {
        text-align: center;
        margin: 0;
        font-size: 7px;
        font-weight: 100;
        cursor: pointer;
        color: #eee;
        position: absolute;
        top: 14px;
        left: 0;
        right: 0;
      }

      #${id}-node img {
        max-width: 100%;
        max-height: 100%;
        height: 100%;
        width: auto;
      }
      </style>
    </head>
    <div id="${id}-node">
      ${n.icon ? `<img src="/${n.icon}" />` : ""}
      <h1>${n.name}</h1>
    </div>
  </html>`;
};

const calculateNodeDimensions = (d?: GraphNode) => {
  return {
    width: 80,
    height: 80,
  };
};

const renderTooltip = (flow: Flow): string => {
  const id = flow.id;

  return `<html>
    <head>
      <style>
      #${id}-flow {
        font-family: "Roboto","Helvetica","Arial",sans-serif;
        background: #222;
        border: 2px solid #1f1f1f;
        color: #eee;
        display: flex;
        justify-content: center;
      }

      #${id}-flow table {
        width: auto;
        table-layout: fixed;
        border-collapse: collapse;
        font-size: 12px;
        text-align: left;
      }

      #${id}-flow table td, #${id}-flow th {
        padding: 2px;
      }
      </style>
    </head>
    <div id="${id}-flow" class="${flow.failures ? 'flow-failures' : ''}">
      <table>
        <tr>
          <th>Source</th>
          <td>${flow.source.replace("n-", "")}</td>
        </tr>
        <tr>
          <th>Target</th>
          <td>${flow.target.replace("n-", "")}</td>
        </tr>
        <tr>
          <th>Status</th>
          <td>${
            flow.failures === 0
              ? "Nominal"
              : `<span style="color: #ff9999">${flow.failures}/${flow.runs} job failures</span>`
          }</td>
        </tr>
        <tr>
          <th>Throughput</th>
          <td>${flow.throughput}k records/day</td>
        </tr>
        ${
          flow.failures
            ? `<tr>
          <th>Error</th>
          <td>${(flow.lastFailureMessage || "").substring(0, 20)}...</td>
        </tr>`
            : ""
        }
      </table>
    </div>
  </html>`;
};
