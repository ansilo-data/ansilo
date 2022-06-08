import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";

import type { AppState, AppThunk } from "../../store/store";
import { Job } from "../jobs/jobs.slice";
import { fetchNodes, isAuthoritative } from "./catalog.api";

export interface Node {
  name: string;
  id: Id;
  url: string;
  icon?: string;
  schema: Schema;
  tags: Tag[];
  jobs: Job[];
}

export interface Schema {
  entities: EntitySchema[];
}

export interface EntitySchema {
  id: Id;
  name: string;
  description: string;
  tags: TagValue[];
  versions: EntitySchemaVersion[];
}

export interface Tag {
  id: Id;
  name: string;
  description: string;
}

export interface TagValue {
  key: Id;
  value: string;
}

export interface EntitySchemaVersion {
  id: Id;
  version: VersionNumber;
  attributes: EntitySchemaAttribute[];
  constraints?: Constraint[];
}

export interface VersionNumber {
  major: number;
  minor: number;
  patch: number;
}

export interface EntitySchemaAttribute {
  id: Id;
  name: string;
  description: string;
  type: DataType;
  validations?: Validation[];
}

export interface DataType {
  // TODO: refine types and params
  name: string;
}

export interface Constraint {
  id: Id;
  type: "fk" | "unique";
  attributes: Id[];
  // fk attributes
  targetEntity?: Id;
  targetAttributes?: Id[];
}

export interface Validation {
  name: string;
}

// export interface Relation {
//     id: Id
//     label: string
//     type: RelationType
//     entities: [Id, Id]
//     foreignKeys: Id[]
// }

// export interface RelationType {

// }

export type Id = string;

export interface CatalogState {
  nodes?: Node[];
  status: "idle" | "loading" | "failed";
}

const initialState: CatalogState = {
  status: "idle",
};

export const fetchCatalogAsync = createAsyncThunk("catalog/fetch", async () => {
  const response = await fetchNodes();
  return {
    nodes: response,
  };
});

export const catalogSlice = createSlice({
  name: "catalog",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchCatalogAsync.pending, (state) => {
        state.status = "loading";
      })
      .addCase(fetchCatalogAsync.fulfilled, (state, action) => {
        state.status = "idle";
        state.nodes = action.payload.nodes;
      })
      .addCase(fetchCatalogAsync.rejected, (state, action) => {
        state.status = "failed";
      });
  },
});

// export const {} = catalogSlice.actions;

export const selectCatalog = (state: AppState) => state.catalog;
export const selectAuthoritativeNode = (state: AppState) =>
  state.catalog.nodes?.find(isAuthoritative);

export default catalogSlice.reducer;