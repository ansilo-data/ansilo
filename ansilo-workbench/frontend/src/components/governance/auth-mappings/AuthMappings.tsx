import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import LoadingButton from "@mui/lab/LoadingButton";
import GovernanceMenu from "../GovernanceMenu";
import { AuthMappingsTable } from "./AuthMappingsTable";

export const AuthMappings = () => {
  return (
    <Box sx={{ flexGrow: "1", display: "flex" }}>
      <GovernanceMenu />
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
          <AuthMappingsTable />
        </Paper>
      </Container>
    </Box>
  );
};
