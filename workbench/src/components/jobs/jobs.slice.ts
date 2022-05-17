import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { AppState, AppThunk } from "../../store/store";

export interface Job {
  name: string;
  description: string;
  id: Id;
  query: JobQuery;
  destination: JobDestination;
  trigger: JobTrigger; // todo: multiple
}

export interface JobQuery {
  sql: string;
}

export interface JobDestination {
  type: "table" | "file";
  options: any;
}

export interface JobTrigger {
  type: "schedule" | "manual" | "after";
  options: any;
}

export type Id = string;

export interface JobState {
  jobs: Job[];
}

const initialState: JobState = {
  jobs: [],
};

export const jobSlice = createSlice({
  name: "job",
  initialState,
  reducers: {
    createJob: (state, action: PayloadAction<Job>) => {
      state.jobs.push(action.payload);
    },
  },
  extraReducers: (builder) => {},
});

export const { createJob } = jobSlice.actions;

export const selectJobs = (state: AppState) => state.jobs;

export default jobSlice.reducer;
