import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";

import type { AppState, AppThunk } from "../../store/store";
import { fetchAuthMethods } from "./auth.api";

const CREDENTIALS_KEY = "AUTH_CREDS";
const METHOD_KEY = "AUTH_LOGIN_METHOD";

export interface AuthState {
  modalOpen: boolean;
  authRequiredSem: number;
  methodsStatus: "idle" | "loading" | "failed";
  methods?: AuthMethod[];
  methodId?: string;
  creds?: AuthCredentials;
  hasInit: boolean;
}

export interface AuthMethod {
  id: string;
  name: string;
  usernames: string[];
  type: "username_password" | "jwt" | "saml";
  options: {
    type?: "oauth2";
    authorize_endpoint?: string;
    params?: { [i: string]: string };
    entity_id?: string;
  };
}

export interface AuthCredentials {
  username: string;
  password: string;
  expiresAt?: number;
}

export const fetchAuthMethodsAsync = createAsyncThunk(
  "auth/providers/fetch",
  async () => {
    const response = await fetchAuthMethods();
    return {
      methods: response,
    };
  }
);

const getInitialState = (): AuthState => {
  return {
    modalOpen: false,
    authRequiredSem: 0,
    methodsStatus: "idle",
    hasInit: false,
  };
};

export const authSlice = createSlice({
  name: "auth",
  initialState: getInitialState,
  reducers: {
    setModalOpen: (state, action: PayloadAction<boolean>) => {
      state.modalOpen = action.payload;
    },
    setMethodId: (state, action: PayloadAction<string | undefined>) => {
      sessionStorage.setItem(METHOD_KEY, action.payload || "");
      state.methodId = action.payload;
    },
    setCredentials: (state, action: PayloadAction<AuthCredentials>) => {
      sessionStorage.setItem(CREDENTIALS_KEY, JSON.stringify(action.payload));
      window.location.hash = "";
      state.creds = action.payload;
      state.modalOpen = false;
    },
    clearCredentials: (state, action: PayloadAction<any>) => {
      sessionStorage.removeItem(CREDENTIALS_KEY);
      state.creds = undefined;
    },
    incrementAuthRequired: (state, action: PayloadAction<any>) => {
      state.authRequiredSem++;
    },
    decrementAuthRequired: (state, action: PayloadAction<any>) => {
      state.authRequiredSem--;
    },
    loadInitialState: (state, action: PayloadAction<any>) => {
      let methodId = sessionStorage.getItem(METHOD_KEY) || undefined;
      let credsJson = sessionStorage.getItem(CREDENTIALS_KEY);
      let credentials = undefined;

      if (credsJson) {
        try {
          credentials = JSON.parse(credsJson);
        } catch (e) {
          console.warn(`Failed to parse auth creds from sessionStorage: ${e}`);
        }
      }

      state.methodId = methodId;
      state.creds = credentials;
      state.hasInit = true;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchAuthMethodsAsync.pending, (state) => {
        state.methodsStatus = "loading";
      })
      .addCase(fetchAuthMethodsAsync.fulfilled, (state, action) => {
        state.methodsStatus = "idle";
        state.methods = action.payload.methods;
      })
      .addCase(fetchAuthMethodsAsync.rejected, (state, action) => {
        state.methodsStatus = "failed";
      });
  },
});

export const {
  setModalOpen,
  setMethodId,
  setCredentials,
  clearCredentials,
  incrementAuthRequired,
  decrementAuthRequired,
  loadInitialState,
} = authSlice.actions;

export const selectAuth = (state: AppState) => state.auth;
export const selectCredentials = (state: AppState) => {
  if (!state.auth.creds) {
    return undefined;
  }

  if (state.auth.creds.expiresAt && Date.now() >= state.auth.creds.expiresAt) {
    return undefined;
  }

  return state.auth.creds;
};

export default authSlice.reducer;
