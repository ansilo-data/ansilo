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

export const DataGrantsTable = () => {
  return (
    <TableContainer sx={{minWidth: 500}}>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Entity</TableCell>
            <TableCell width="50%">Operations</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>Contacts</TableCell>
            <TableCell>SELECT, INSERT, UPDATE, DELETE</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Interactions</TableCell>
            <TableCell>SELECT</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
};
