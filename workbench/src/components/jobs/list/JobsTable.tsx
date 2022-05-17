import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { styled } from "@mui/material/styles";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import { Job } from "../jobs.slice";

interface Props {
  jobs: Job[];
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

export const JobsTable = ({ jobs }: Props) => {
  if (jobs.length === 0) {
    return (
      <MessageBox>
        <Typography variant="body1">No jobs have been defined</Typography>
      </MessageBox>
    );
  }

  return (
    <Box sx={{ width: "100%" }}>
      <TableContainer sx={{ width: "100%" }}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Trigger</TableCell>
              <TableCell>Destination</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobs.map((j) => (
              <TableRow key={j.id}>
                <TableCell>{j.name}</TableCell>
                <TableCell>{j.description}</TableCell>
                <TableCell>{j.trigger.type}</TableCell>
                <TableCell>{j.destination.type}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};
