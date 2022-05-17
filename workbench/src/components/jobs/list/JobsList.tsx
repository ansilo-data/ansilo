import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import { useAppDispatch, useAppSelector } from "../../../store/hooks";
import { selectJobs } from "../jobs.slice";
import Link from "next/link";
import Button from "@mui/material/Button";
import { JobsTable } from "./JobsTable";

export const JobsList = () => {
  const dispatch = useAppDispatch();
  const jobs = useAppSelector(selectJobs);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        justifyContent: "center",
        py: 2,
      }}
    >
      <Container>
        <Paper
          sx={{
            display: "flex",
            flexDirection: "column",
            width: "100%",
            flexGrow: 1,
            p: 2,
          }}
          elevation={8}
        >
          <Box sx={{ display: "flex", justifyContent: "right" }}>
            <Link href="/jobs/create">
              <Button variant="contained">Create Job</Button>
            </Link>
          </Box>
          <Divider sx={{my: 2}} />
          <JobsTable jobs={jobs.jobs} />
        </Paper>
      </Container>
    </Box>
  );
};
