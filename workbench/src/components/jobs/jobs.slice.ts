import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { AppState, AppThunk } from "../../store/store";
import { selectAuthoritativeNode } from "../catalog/catalog.slice";

export interface Job {
  name: string;
  description: string;
  id: Id;
  query: JobQuery;
  destination: JobDestination;
  trigger: JobTrigger; // todo: multiple
  runs: JobResult[];
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

export interface JobResult {
  status: "success" | "error";
  message?: string;
  recordsCount?: number;
}

export type Id = string;

export interface JobState {
  newJobs: Job[];
}

const initialState: JobState = { newJobs: [] };

export const jobSlice = createSlice({
  name: "job",
  initialState,
  reducers: {
    createJob: (state, action: PayloadAction<Job>) => {
      state.newJobs.push(action.payload);
    },
  },
  extraReducers: (builder) => {},
});

export const { createJob } = jobSlice.actions;

export const selectJobs = (state: AppState) =>
  (selectAuthoritativeNode(state)?.jobs || []).concat(state.jobs.newJobs);

export default jobSlice.reducer;
