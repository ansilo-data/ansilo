import { configureStore, ThunkAction, Action } from "@reduxjs/toolkit";
import authReducer from "../components/auth/auth.slice";
import catalogReducer from "../components/catalog/catalog.slice";
import sqlReducer from "../components/sql/sql.slice";
import jobsReducer from "../components/jobs/jobs.slice";

export function makeStore() {
  return configureStore({
    reducer: { auth: authReducer, catalog: catalogReducer, sql: sqlReducer, jobs: jobsReducer },
  });
}

const store = makeStore();

export type AppState = ReturnType<typeof store.getState>;

export type AppDispatch = typeof store.dispatch;

export type AppThunk<ReturnType = void> = ThunkAction<
  ReturnType,
  AppState,
  unknown,
  Action<string>
>;

export default store;
