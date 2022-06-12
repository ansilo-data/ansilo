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
import TableRow from "@mui/material/TableRow";

export const SystemGrantsTable = () => {
  return (
    <TableContainer>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Operation</TableCell>
            <TableCell width="50%">Permission</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>Execute Query</TableCell>
            <TableCell>Granted</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>View Jobs</TableCell>
            <TableCell>Granted</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>View Roles</TableCell>
            <TableCell>Granted</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>View Operations</TableCell>
            <TableCell>Denied</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
};
