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

export const RolesTable = () => {
  return (
    <TableContainer>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Name</TableCell>
            <TableCell>Description</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>Administrator</TableCell>
            <TableCell>Grants full access to all data and operations</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Analyst</TableCell>
            <TableCell>Grants read-only access to metadata and ability to execute queries</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Support</TableCell>
            <TableCell>Read-only access to operational data such as audit and system logs</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Data Consumer</TableCell>
            <TableCell>Grants read-only access to metadata and ability to execute queries</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
};
