import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import GovernanceMenu from "../GovernanceMenu";

export const AuthoritiesTable = () => {
  return (
    <TableContainer>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Type</TableCell>
            <TableCell>Name</TableCell>
            <TableCell>Host</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>User</TableCell>
            <TableCell>Microsoft Azure AD</TableCell>
            <TableCell>organisation.onmicrosoft.com</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>System</TableCell>
            <TableCell>Token Service</TableCell>
            <TableCell>token-service.organisation</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
};
