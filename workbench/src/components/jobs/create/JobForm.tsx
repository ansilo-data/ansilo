import * as React from "react";
import Box from "@mui/material/Box";
import Stepper from "@mui/material/Stepper";
import Step from "@mui/material/Step";
import StepLabel from "@mui/material/StepLabel";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { Job } from "../jobs.slice";
import _ from "lodash";

interface Props {
  job?: Job;
  onSubmit: (job: Job) => void;
}

export const JobForm = (props: Props) => {
  const [job, setJob] = React.useState(
    props.job || {
      id: "",
      name: "",
      description: "",
      query: { sql: "" },
      destination: { type: "table" },
      trigger: { type: "manual" },
    }
  );
  const [activeStep, setActiveStep] = React.useState(0);

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
  };

  const updateJob = (updates: Partial<Job>) => {
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
      <React.Fragment>
        <Typography sx={{ mt: 2, mb: 1 }}>Step {activeStep + 1}</Typography>
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
          <Button onClick={handleNext}>
            {activeStep === 4 ? "Finish" : "Next"}
          </Button>
        </Box>
      </React.Fragment>
    </Box>
  );
};
