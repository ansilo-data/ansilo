import * as React from "react";
import TreeView from "@mui/lab/TreeView";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import NodeIcon from "@mui/icons-material/StorageOutlined";
import TreeItem from "@mui/lab/TreeItem";
import EntityIcon from "@mui/icons-material/TableChartOutlined";
import VersionIcon from "@mui/icons-material/ArrowRightOutlined";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import {
  fetchCatalogAsync,
  selectCatalog,
  Node,
  EntitySchema,
} from "./catalog.slice";
import { styled } from "@mui/material/styles";
import { versionLabel } from "../../util/versionLabel";
import Typography from "@mui/material/Typography";
import { navigationWidth } from "./Catalog";
import Box from "@mui/material/Box";
import _ from "lodash";

const StyledTreeItem = styled(TreeItem)(({ theme }) => ({
  "& .MuiTreeItem-content": {
    padding: 12,
  },
}));

const Note = styled(Typography)(({ theme }) => ({
  "&": {
    fontSize: 12,
    color: theme.palette.grey["500"],
    display: "inline",
  },
}));

const VendorIcon = styled("img")(({ theme }) => ({
  "&": {
    marginLeft: "auto",
    paddingLeft: 16,
    height: 16,
  },
}));

interface Props {
  categorisation: "node" | string;
  narrow?: boolean;
  onClick: (versionId: string) => void;
}

export default function CatalogTreeView(props: Props) {
  const catalog = useAppSelector(selectCatalog);
  const isAuthoritative = (node: Node) =>
    node.url.startsWith(window.location.origin);

  const renderEntity = (e: EntitySchema) => {
    return (
      <StyledTreeItem
        icon={<EntityIcon />}
        key={e.id}
        nodeId={e.id}
        label={e.name}
      >
        {e.versions.map((v) => (
          <StyledTreeItem
            icon={<VersionIcon />}
            key={v.id}
            nodeId={v.id}
            label={versionLabel(v.version)}
            onClick={() => props.onClick(v.id)}
          />
        ))}
      </StyledTreeItem>
    );
  };

  if (props.categorisation === "node") {
    return (
      <TreeView
        defaultCollapseIcon={<ExpandMoreIcon />}
        defaultExpandIcon={<ChevronRightIcon />}
        sx={{ height: "100%", flexGrow: 1, width: "100%", overflowY: "auto" }}
      >
        {catalog.nodes?.map((i) => (
          <StyledTreeItem
            icon={<NodeIcon />}
            key={i.id}
            nodeId={i.id}
            label={
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                {i.name}{" "}
                {isAuthoritative(i) ? (
                  <Note sx={{ pl: 1 }}>(Authoritative)</Note>
                ) : null}
                {i.icon && !props.narrow && (
                  <>
                    <VendorIcon src={i.icon} />
                  </>
                )}
              </Box>
            }
          >
            {i.schema.entities.map(renderEntity)}
          </StyledTreeItem>
        ))}
      </TreeView>
    );
  } else {
    const tagValues = _.sortedUniq(
      catalog.nodes
        ?.flatMap((i) =>
          i.schema.entities.flatMap((e) =>
            e.tags.filter((t) => t.key === props.categorisation)
          )
        )
        .map((t) => t.value) || []
    );

    return (
      <TreeView
        defaultCollapseIcon={<ExpandMoreIcon />}
        defaultExpandIcon={<ChevronRightIcon />}
        sx={{ height: "100%", flexGrow: 1, width: "100%", overflowY: "auto" }}
      >
        {tagValues.map((tv) => (
          <StyledTreeItem
            icon={<NodeIcon />}
            key={tv}
            nodeId={`tag-${tv}`}
            label={
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                {tv}{" "}
              </Box>
            }
          >
            {catalog.nodes
              ?.flatMap((n) =>
                n.schema.entities.filter((e) =>
                  e.tags.some(
                    (t) => t.key === props.categorisation && t.value === tv
                  )
                )
              )
              .map(renderEntity)}
          </StyledTreeItem>
        ))}
      </TreeView>
    );
  }
}
