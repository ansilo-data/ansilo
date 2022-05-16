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
  onClick: (query: Query) => void;
  pastQueries: Query[];
}

export const QueryHistory = ({ pastQueries, onClick }: Props) => {
  return (
    <Box sx={{ width: "100%" }}>
      <TableContainer sx={{ width: "100%" }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Query</TableCell>
              <TableCell>Result</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {[...pastQueries].reverse().map((q, i) => (
              <TableRow
                key={i}
                onClick={() => onClick(q)}
                sx={{ cursor: "pointer" }}
              >
                <TableCell
                  sx={{
                    overflow: "hidden",
                    wordBreak: "break-all",
                    textOverflow: "ellipsis",
                    width: "70%",
                  }}
                >
                  {q.sql.substring(0, 50)}
                  {q.sql.length > 50 ? "..." : ""}
                </TableCell>
                <TableCell sx={{ width: "30%", wordBreak: "break-all" }}>
                  {q.errorMessage || `${q.results?.values.length || 0} rows`}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};
