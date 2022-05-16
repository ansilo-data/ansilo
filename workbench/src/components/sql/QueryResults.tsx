import Box from "@mui/material/Box";
import { Query, QueryStatus } from "./sql.slice";
import { QueryEditor } from "./QueryEditor";
import { QueryToolbar } from "./QueryToolbar";
import Alert from "@mui/material/Alert";
import AlertTitle from "@mui/material/AlertTitle";
import Typography from "@mui/material/Typography";
import { styled } from "@mui/material/styles";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";

interface Props {
  query: Query;
}

const MessageBox = styled(
  Box,
  {}
)(({ theme }) => ({
  "&": {
    width: "100%",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
  },
  "& .MuiTypography-body1": {
    color: theme.palette.grey["500"],
  },
}));

export const QueryResults = ({ query }: Props) => {
  if (query.status === QueryStatus.Failed) {
    return (
      <MessageBox>
        <Alert severity="error">
          <AlertTitle>Error</AlertTitle>
          {query.errorMessage}
        </Alert>
      </MessageBox>
    );
  }

  if (query.status === QueryStatus.Incomplete) {
    return (
      <MessageBox>
        <Typography variant="body1">Query has not executed</Typography>
      </MessageBox>
    );
  }

  if (query.status === QueryStatus.Executing) {
    return (
      <MessageBox>
        <Typography variant="body1">Query is executing...</Typography>
      </MessageBox>
    );
  }

  if (!query.results) {
    return (
      <MessageBox>
        <Typography variant="body1">Query returned no results...</Typography>
      </MessageBox>
    );
  }

  return (
    <Box sx={{ width: "100%" }}>
      <TableContainer sx={{ width: "100%" }}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              {query.results.columns.map((c) => (
                <TableCell key={c}>{c}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {query.results.values.map((r, i) => (
              <TableRow key={i}>
                {r.map((c) => (
                  <TableCell key={c}>{c}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};
