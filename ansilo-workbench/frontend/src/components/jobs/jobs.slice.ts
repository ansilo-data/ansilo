import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { AppState, AppThunk } from "../../store/store";
import { selectCredentials } from "../auth/auth.slice";
import { selectAuthoritativeNode } from "../catalog/catalog.slice";
import { fetchJobs } from "./jobs.api";

export interface Job {
  name: string;
  description: string;
  serviceUserId: string;
  id: Id;
  query: JobQuery;
  trigger: string;
  runs: JobResult[];
}

export interface JobQuery {
  sql: string;
}

export interface JobResult {
  status: "success" | "error";
  message?: string;
  recordsCount?: number;
}

export type Id = string;

export interface JobState {
  status: "idle" | "loading" | "failed";
  jobs: Job[];
}

export const fetchJobsAsync = createAsyncThunk(
  "job/fetch",
  async (_, { getState, dispatch }) => {
    const state = getState() as AppState;
    const creds = selectCredentials(state);
    const response = await fetchJobs(dispatch as any, creds!);
    return {
      jobs: response,
    };
  }
);

const initialState: JobState = { status: "idle", jobs: [] };

export const jobSlice = createSlice({
  name: "job",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchJobsAsync.pending, (state) => {
        state.status = "loading";
      })
      .addCase(fetchJobsAsync.fulfilled, (state, action) => {
        state.status = "idle";
        state.jobs = action.payload.jobs;
      })
      .addCase(fetchJobsAsync.rejected, (state, action) => {
        state.status = "failed";
      });
  },
});

export const selectJobs = (state: AppState) => state.jobs.jobs;

export default jobSlice.reducer;
