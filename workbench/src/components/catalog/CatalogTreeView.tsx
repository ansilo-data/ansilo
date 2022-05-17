import * as React from "react";
import TreeView from "@mui/lab/TreeView";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import NodeIcon from "@mui/icons-material/StorageOutlined";
import TreeItem from "@mui/lab/TreeItem";
import EntityIcon from "@mui/icons-material/TableChartOutlined";
import VersionIcon from "@mui/icons-material/ArrowRightOutlined";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import { fetchCatalogAsync, selectCatalog, Node } from "./catalog.slice";
import { styled } from "@mui/material/styles";
import { versionLabel } from "../../util/versionLabel";
import Typography from "@mui/material/Typography";

const StyledTreeItem = styled(TreeItem)(({ theme }) => ({
  "& .MuiTreeItem-content": {
    padding: 12,
  },
}));

const Note = styled(Typography)(({ theme }) => ({
  "&": {
    fontSize: 12,
    color: theme.palette.grey["500"],
    display: 'inline'
  },
}));

interface Props {
  onClick: (versionId: string) => void;
}

export default function CatalogTreeView(props: Props) {
  const catalog = useAppSelector(selectCatalog);
  const isAuthoritative = (node: Node) =>
    node.url.startsWith(window.location.origin);

  return (
    <TreeView
      defaultCollapseIcon={<ExpandMoreIcon />}
      defaultExpandIcon={<ChevronRightIcon />}
      sx={{ height: "100%", flexGrow: 1, maxWidth: 240, overflowY: "auto" }}
    >
      {catalog.nodes?.map((i) => (
        <StyledTreeItem
          icon={<NodeIcon />}
          key={i.id}
          nodeId={i.id}
          label={
            <>
              {i.name}{" "}
              {isAuthoritative(i) ? <Note>(Authoritative)</Note> : null}
            </>
          }
        >
          {i.schema.entities.map((e) => (
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
          ))}
        </StyledTreeItem>
      ))}
    </TreeView>
  );
}
