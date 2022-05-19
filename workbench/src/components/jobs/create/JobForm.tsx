import * as React from "react";
import Box from "@mui/material/Box";
import Stepper from "@mui/material/Stepper";
import Step from "@mui/material/Step";
import StepLabel from "@mui/material/StepLabel";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import { Job, selectJobs } from "../jobs.slice";
import _ from "lodash";
import TextField from "@mui/material/TextField";
import { useAppSelector } from "../../../store/hooks";
import { QueryEditor } from "../../sql/QueryEditor";
import { selectCatalog } from "../../catalog/catalog.slice";
import MenuItem from "@mui/material/MenuItem";
import { DeepPartial } from "@reduxjs/toolkit";
import { styled } from "@mui/material/styles";

interface Props {
  job?: Job;
  onSubmit: (job: Job) => void;
}

export const JobForm = (props: Props) => {
  const [job, setJob] = React.useState<Job>(
    props.job || {
      id: "",
      name: "",
      description: "",
      query: { sql: "" },
      destination: { type: "table", options: {} },
      trigger: { type: "manual", options: {} },
    }
  );
  const [activeStep, setActiveStep] = React.useState(0);

  const handleNext = () => {
    if (activeStep === 4) {
      props.onSubmit(job);
      return;
    }
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
  };

  const updateJob = (updates: DeepPartial<Job>) => {
    setJob(_.defaultsDeep(updates, job));
  };

  return (
    <Box sx={{ width: "100%" }}>
      <Stepper activeStep={activeStep}>
        <Step>
          <StepLabel>General options</StepLabel>
        </Step>
        <Step>
          <StepLabel>Source</StepLabel>
        </Step>
        <Step>
          <StepLabel>Destination</StepLabel>
        </Step>
        <Step>
          <StepLabel>Trigger</StepLabel>
        </Step>
        <Step>
          <StepLabel>Review</StepLabel>
        </Step>
      </Stepper>
      <Box
        component="form"
        sx={{
          pt: 4,
          display: "flex",
          flexDirection: "column",
        }}
        onSubmit={(e: React.FormEvent<any>) => {
          e.preventDefault();
          handleNext();
        }}
      >
        {activeStep === 0 && (
          <GeneralSettings job={job} updateJob={updateJob} />
        )}
        {activeStep === 1 && <Source job={job} updateJob={updateJob} />}
        {activeStep === 2 && <Destination job={job} updateJob={updateJob} />}
        {activeStep === 3 && <Trigger job={job} updateJob={updateJob} />}
        {activeStep === 4 && <Review job={job} />}
        <Box sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
          <Button
            color="inherit"
            disabled={activeStep === 0}
            onClick={handleBack}
            sx={{ mr: 1 }}
          >
            Back
          </Button>
          <Box sx={{ flex: "1 1 auto" }} />
          <Button type="submit">{activeStep === 4 ? "Finish" : "Next"}</Button>
        </Box>
      </Box>
    </Box>
  );
};

interface StepProps {
  job: Job;
  updateJob: (job: DeepPartial<Job>) => void;
}

const GeneralSettings = (props: StepProps) => {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        "& > *:not(:last-child)": { pb: 2 },
      }}
    >
      <TextField
        required
        label="Name"
        value={props.job.name}
        onChange={(e) => props.updateJob({ name: e.target.value })}
      />
      <TextField
        multiline
        rows={5}
        label="Description"
        value={props.job.description}
        onChange={(e) => props.updateJob({ description: e.target.value })}
      />
    </Box>
  );
};

const Source = (props: StepProps) => {
  const catalog = useAppSelector(selectCatalog);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: 300,
      }}
    >
      <QueryEditor
        required
        sql={props.job.query.sql}
        catalog={catalog}
        onChange={(sql) => props.updateJob({ query: { sql } })}
      />
    </Box>
  );
};

const Destination = (props: StepProps) => {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        "& > *:not(:last-child)": { pb: 2 },
      }}
    >
      <TextField
        required
        select
        label="Type"
        value={props.job.destination.type}
        onChange={(e) =>
          props.updateJob({ destination: { type: e.target.value as any } })
        }
      >
        <MenuItem value="table">Database Table</MenuItem>
        <MenuItem value="file">File</MenuItem>
      </TextField>
      {props.job.destination.type === "table" && (
        <>
          <TextField
            required
            label="Table Name"
            value={props.job.destination.options?.tableName}
            onChange={(e) =>
              props.updateJob({
                destination: { options: { tableName: e.target.value as any } },
              })
            }
          />
        </>
      )}
      {props.job.destination.type === "file" && (
        <>
          <TextField
            required
            select
            label="File Format"
            value={props.job.destination.options?.format}
            onChange={(e) =>
              props.updateJob({
                destination: { options: { format: e.target.value as any } },
              })
            }
          >
            <MenuItem value="csv">CSV</MenuItem>
            <MenuItem value="parquet">Parquet</MenuItem>
            <MenuItem value="nd-json">ND-JSON</MenuItem>
          </TextField>
          <TextField
            required
            label="File Path"
            value={props.job.destination.options?.filePath}
            onChange={(e) =>
              props.updateJob({
                destination: { options: { filePath: e.target.value as any } },
              })
            }
          />
        </>
      )}
    </Box>
  );
};

const Trigger = (props: StepProps) => {
  const jobs = useAppSelector(selectJobs);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        "& > *:not(:last-child)": { pb: 2 },
      }}
    >
      <TextField
        required
        select
        label="Type"
        value={props.job.trigger.type}
        onChange={(e) =>
          props.updateJob({ trigger: { type: e.target.value as any } })
        }
      >
        <MenuItem value="manual">Manual</MenuItem>
        <MenuItem value="schedule">Scheduled</MenuItem>
        <MenuItem value="after">After another job</MenuItem>
      </TextField>
      {props.job.trigger.type === "schedule" && (
        <>
          <TextField
            required
            label="Cron Expression"
            placeholder="Format: m h d m w y (eg 0 0 * * ? *)"
            value={props.job.trigger.options?.cron}
            onChange={(e) =>
              props.updateJob({
                trigger: { options: { cron: e.target.value as any } },
              })
            }
          />
        </>
      )}
      {props.job.trigger.type === "manual" && (
        <>
          <Typography variant="body1">
            This job will only be executed by manual trigger or via API
          </Typography>
        </>
      )}
      {props.job.trigger.type === "after" && (
        <>
          <TextField
            required
            select
            label="Job"
            value={props.job.trigger.options?.jobId}
            onChange={(e) =>
              props.updateJob({
                trigger: { options: { jobId: e.target.value as any } },
              })
            }
          >
            {jobs.jobs.map((j) => (
              <MenuItem key={j.id} value={j.id}>
                {j.name}
              </MenuItem>
            ))}
          </TextField>
        </>
      )}
    </Box>
  );
};

const SectionCell = styled(TableCell, {})(({theme}) => ({
  '&': {
    textAlign: 'center',
    fontSize: 14,
    color: theme.palette.grey["400"],
    textTransform: 'uppercase',
    paddingTop: 30
  }
}))

const Review = ({ job }: { job: Job }) => {
  return (
    <Box>
      <TableContainer>
        <Table>
          <TableBody>
            <TableRow>
              <SectionCell colSpan={2}>General</SectionCell>
            </TableRow>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>{job.name}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell>Description</TableCell>
              <TableCell>{job.description}</TableCell>
            </TableRow>
            <TableRow>
              <SectionCell colSpan={2}>Source</SectionCell>
            </TableRow>
            <TableRow>
              <TableCell>Query</TableCell>
              <TableCell>{job.query.sql}</TableCell>
            </TableRow>
            <TableRow>
              <SectionCell colSpan={2}>Destination</SectionCell>
            </TableRow>
            <TableRow>
              <TableCell>Type</TableCell>
              <TableCell>{job.destination.type}</TableCell>
            </TableRow>
            {_.toPairs(job.destination.options).map(([k, v]) => (
              <TableRow key={k}>
                <TableCell>{k}</TableCell>
                <TableCell>{v as any}</TableCell>
              </TableRow>
            ))}
            <TableRow>
              <SectionCell colSpan={2}>Trigger</SectionCell>
            </TableRow>
            <TableRow>
              <TableCell>Type</TableCell>
              <TableCell>{job.trigger.type}</TableCell>
            </TableRow>
            {_.toPairs(job.trigger.options).map(([k, v]) => (
              <TableRow key={k}>
                <TableCell>{k}</TableCell>
                <TableCell>{v as any}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};
