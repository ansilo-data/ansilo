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

export const AuthMappingsTable = () => {
  return (
    <TableContainer>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Name</TableCell>
            <TableCell>Type</TableCell>
            <TableCell>Description</TableCell>
            <TableCell>Roles</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>Administrator User Access</TableCell>
            <TableCell>AD Group</TableCell>
            <TableCell>{'Group "Data Admins"'}</TableCell>
            <TableCell>Administrator</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Analyst User Access</TableCell>
            <TableCell>AD Group</TableCell>
            <TableCell>{'Group "Data Analysts"'}</TableCell>
            <TableCell>Analyst</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Support User Access</TableCell>
            <TableCell>AD Group</TableCell>
            <TableCell>{'Group "Data Developers"'}</TableCell>
            <TableCell>Support</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Data Consumer Node</TableCell>
            <TableCell>Token Claims</TableCell>
            <TableCell>
              {'"scopes" claim contains "CONTACTS.VIEW"'}
            </TableCell>
            <TableCell>Data Consumer</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
};
