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
import { useAppDispatch, useAppSelector } from "../../../store/hooks";
import { fetchAuthMethodsAsync, selectAuth } from "../../auth/auth.slice";
import { useEffect } from "react";
import { Authenticated } from "../../auth/Authenticated";

export const AuthoritiesTable = () => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth);

  useEffect(() => {
    dispatch(fetchAuthMethodsAsync())
  }, [])

  const methods = auth.methods?.filter(i => i.type !== 'username_password');

  return (
    <Authenticated>
      <TableContainer>
        <Table stickyHeader sx={{ minWidth: 300 }}>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Type</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {
              methods?.length
                ?
                methods.map(method => (
                  <TableRow >
                    <TableCell>{method.name}</TableCell>
                    <TableCell>{method.type.toUpperCase()}</TableCell>
                  </TableRow>
                ))
                : <TableRow>
                  <TableCell colSpan={3}>No authentication providers are configured</TableCell>
                </TableRow>
            }
          </TableBody>
        </Table>
      </TableContainer>
    </Authenticated>
  );
};
