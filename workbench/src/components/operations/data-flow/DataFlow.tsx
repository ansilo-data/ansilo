import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import OperationsMenu from "../OperationsMenu";
import { DataFlowDiagram } from "./DataFlowDiagram";
import { selectCatalog } from "../../catalog/catalog.slice";
import { useAppSelector } from "../../../store/hooks";

export const DataFlow = () => {
  const catalog = useAppSelector(selectCatalog);

  return (
    <Box sx={{ flexGrow: "1", display: "flex" }}>
      <OperationsMenu />
      <Container
        sx={{
          maxWidth: 800,
          flexGrow: 1,
          display: "flex",
          justifyContent: "center",
          padding: 4,
        }}
      >
        <Paper
          sx={{
            display: "flex",
            width: "100%",
            height: "100%",
            alignItems: "center",
          }}
          elevation={8}
        >
          {catalog.nodes && <DataFlowDiagram nodes={catalog.nodes} />}
        </Paper>
      </Container>
    </Box>
  );
};
