import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import { useAppDispatch, useAppSelector } from "../../../store/hooks";
import { createJob, selectJobs } from "../jobs.slice";
import Link from "next/link";
import Button from "@mui/material/Button";
import { JobForm } from "./JobForm";
import { useRouter } from "next/router";

export const CreateJob = () => {
  const router = useRouter();
  const dispatch = useAppDispatch();
  const jobs = useAppSelector(selectJobs);

  const handleCreateJob = (job: Job) => {
    dispatch(createJob(job));
    router.push("/jobs");
  };

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
          <JobForm onSubmit={(job) => handleCreateJob} />
        </Paper>
      </Container>
    </Box>
  );
};
