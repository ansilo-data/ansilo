import _ from "lodash";
import { API_CONFIG } from "../../config/api";
import { AppDispatch } from "../../store/store";
import { AuthCredentials, AuthMethod, clearCredentials } from "./auth.slice";

export const fetchAuthMethods = async (): Promise<AuthMethod[]> => {
  let response = await fetch(`${API_CONFIG.origin}/api/v1/auth/provider`);
  let auth = await response.json();

  return auth.methods.map(
    (m: any) =>
      ({
        id: `am-${m.id}`,
        name: getAuthMethodName(m),
        usernames: m.usernames,
        type: m.type,
        options: m.options,
      } as AuthMethod)
  );
};

export const validateCredentials = async (
  creds: AuthCredentials
): Promise<boolean> => {
  let response = await fetch(`${API_CONFIG.origin}/api/v1/query`, {
    method: "post",
    headers: { "Content-Type": "application/json", ...getAuthHeaders(creds) },
    body: JSON.stringify({
      sql: "SELECT 1",
    }),
  });
  let body = await response.text();

  if (response.status >= 300) {
    console.log(`Authentication failed`, response, body);
    return false;
  }

  return true;
};

export const authenticatedFetch = async (
  dispatch: AppDispatch,
  creds: AuthCredentials,
  path: string,
  init: RequestInit
): Promise<Response | null> => {
  init.headers = {
    ...(init.headers || {}),
    ...getAuthHeaders(creds),
  };
  let response = await fetch(`${API_CONFIG.origin}${path}`, init);

  if (response.status === 401) {
    console.warn(
      `Authenticated request returned 401, clearing credentials...`,
      response,
      await response.text()
    );
    dispatch(clearCredentials(null));
    return null;
  }

  return response;
};

export const getAuthHeaders = (
  creds: AuthCredentials
): { [idx: string]: string } => {
  return {
    Authorization: `Basic ${btoa(`${creds.username}:${creds.password}`)}`,
  };
};

function getAuthMethodName(m: any): string {
  if (m.type === "username_password") {
    return "Username / Password";
  }

  if (m.type === "jwt") {
    return `JWT Token (${m.id}`;
  }

  if (m.type === "saml") {
    return `SAML (${m.id}`;
  }

  return m.id;
}
