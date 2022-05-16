import { configureStore, ThunkAction, Action } from '@reduxjs/toolkit'
import catalogReducer from '../components/catalog/catalog.slice'
import sqlReducer from '../components/sql/sql.slice'


export function makeStore() {
  return configureStore({
    reducer: { catalog: catalogReducer, sql: sqlReducer },
  })
}

const store = makeStore()

export type AppState = ReturnType<typeof store.getState>

export type AppDispatch = typeof store.dispatch

export type AppThunk<ReturnType = void> = ThunkAction<
  ReturnType,
  AppState,
  unknown,
  Action<string>
>

export default store