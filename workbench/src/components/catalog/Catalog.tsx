import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import CatalogTreeView from "./CatalogTreeView";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import { useAnchor } from "../../util/useAnchor";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import {
  EntitySchema,
  EntitySchemaVersion,
  fetchCatalogAsync,
  selectCatalog,
} from "./catalog.slice";
import Typography from "@mui/material/Typography";
import EntityVersionDetails from "./EntityVersionDetails";
import LoadingButton from "@mui/lab/LoadingButton";

const navigationWidth: number = 240;

export const Catalog = () => {
  const dispatch = useAppDispatch();
  const catalog = useAppSelector(selectCatalog);
  const [anchor, setAnchor] = useAnchor();

  const forceRefresh = () => {
    dispatch(fetchCatalogAsync());
  };

  const [currentEntity, currentVersion] = catalog.nodes
    ?.flatMap((i) => i.schema.entities)
    ?.flatMap((e) =>
      e.versions.map((v) => [e, v] as [EntitySchema, EntitySchemaVersion])
    )
    ?.find(([e, v]) => v.id === anchor) || [undefined, undefined];

  return (
    <Box sx={{ flexGrow: "1", display: "flex" }}>
      <Paper sx={{ maxWidth: navigationWidth, flexGrow: 1 }} elevation={6}>
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
          <CatalogTreeView onClick={(versionId) => setAnchor(versionId)} />
        </List>
      </Paper>
      <Container
        sx={{
          maxWidth: 800,
          flexGrow: 1,
          display: "flex",
          justifyContent: "center",
          padding: 4,
        }}
      >
        <Paper sx={{ display: "flex", p: 4 }} elevation={8}>
          {currentEntity && currentVersion ? (
            <EntityVersionDetails
              entity={currentEntity!}
              version={currentVersion!}
            />
          ) : (
            <Box
              sx={{
                display: "flex",
                justifyContent: "center",
                alignItems: "center",
                flexGrow: 1,
              }}
            >
              <Typography>
                Please select an entity to view the details
              </Typography>
            </Box>
          )}
        </Paper>
      </Container>
    </Box>
  );
};
