import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import CatalogTreeView from "../catalog/CatalogTreeView";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import {
  Node,
  EntitySchema,
  EntitySchemaVersion,
  fetchCatalogAsync,
  selectCatalog,
} from "../catalog/catalog.slice";
import LoadingButton from "@mui/lab/LoadingButton";
import { QueryIDE } from "./QueryIDE";
import { selectSql, updateCurrentQuery } from "./sql.slice";

const navigationWidth: number = 240;

export const Sql = () => {
  const dispatch = useAppDispatch();
  const catalog = useAppSelector(selectCatalog);
  const sql = useAppSelector(selectSql);

  const forceRefresh = () => {
    dispatch(fetchCatalogAsync());
  };

  const handleVersionClick = (versionId: string) => {
    const [node, entity, version] = catalog.nodes
      ?.flatMap((n) =>
        n.schema.entities.map((e) => [n, e] as [Node, EntitySchema])
      )
      ?.flatMap(([n, e]) =>
        e.versions.map(
          (v) => [n, e, v] as [Node, EntitySchema, EntitySchemaVersion]
        )
      )
      ?.find(([n, e, v]) => v.id === versionId) || [
        undefined,
        undefined,
        undefined,
      ];

    if (!version) {
      return;
    }

    const sqlName = version.tableName;
    dispatch(updateCurrentQuery(sql.currentQuery.sql + sqlName));
  };

  return (
    <Box sx={{ flexGrow: "1", display: "flex", height: '100%', overflowY: 'hidden' }}>
      <Paper
        sx={{ width: navigationWidth, flexGrow: 0, flexShrink: 0, zIndex: 10 }}
        elevation={6}
      >
        <Toolbar
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            pl: [2],
            pr: [1],
          }}
        >
          Nodes{" "}
          <LoadingButton
            onClick={forceRefresh}
            loading={catalog.status === "loading"}
          >
            Refresh
          </LoadingButton>
        </Toolbar>
        <Divider />
        <List component="nav">
          <CatalogTreeView categorisation="node" narrow onClick={(versionId) => handleVersionClick(versionId)} />
        </List>
      </Paper>
      <Box
        sx={{
          flexGrow: 1,
          display: "flex",
          height: '100%',
          width: 'calc(100% - 240px)',
        }}
      >
        <QueryIDE />
      </Box>
    </Box>
  );
};
