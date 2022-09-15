import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import _ from "lodash";

import type { AppState, AppThunk } from "../../store/store";
import { selectCredentials } from "../auth/auth.slice";
import { executeQuery } from "./sql.api";

export interface Query {
  sql: string;
  status: QueryStatus;
  errorMessage?: string;
  results?: QueryResults;
}

export enum QueryStatus {
  Incomplete,
  Executing,
  Success,
  Failed,
}

export interface QueryResults {
  columns: string[];
  values: any[][];
}

export interface SqlState {
  currentQuery: Query;
  queryHistory: Query[];
}

const initialState: SqlState = {
  currentQuery: {
    sql: "",
    status: QueryStatus.Incomplete,
  },
  queryHistory: [],
};

export const executeCurrentQueryAsync = createAsyncThunk(
  "sql/execute",
  async (_, { getState, dispatch }) => {
    const state = getState() as AppState;
    const creds = selectCredentials(state);
    return await executeQuery(dispatch as any, creds!, state.sql.currentQuery);
  }
);

export const sqlSlice = createSlice({
  name: "sql",
  initialState,
  reducers: {
    updateCurrentQuery: (state, action: PayloadAction<string>) => {
      state.currentQuery.sql = action.payload;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(executeCurrentQueryAsync.pending, (state) => {
        state.currentQuery.status = QueryStatus.Executing;
        state.currentQuery.errorMessage = undefined;
        state.currentQuery.results = undefined;
      })
      .addCase(executeCurrentQueryAsync.fulfilled, (state, action) => {
        state.currentQuery.status = QueryStatus.Success;
        state.currentQuery.results = action.payload;
        state.queryHistory.push(_.cloneDeep(state.currentQuery));
      })
      .addCase(executeCurrentQueryAsync.rejected, (state, payload) => {
        state.currentQuery.status = QueryStatus.Failed;
        state.currentQuery.errorMessage = payload.error.message;
        state.queryHistory.push(_.cloneDeep(state.currentQuery));
      });
  },
});

export const { updateCurrentQuery } = sqlSlice.actions;

export const selectSql = (state: AppState) => state.sql;

export default sqlSlice.reducer;
