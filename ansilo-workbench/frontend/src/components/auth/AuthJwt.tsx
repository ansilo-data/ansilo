import { AuthMethod, fetchAuthMethodsAsync } from "./auth.slice";
import { CredentialsForm } from "./CredentialsForm";
import qs from 'qs'
import Box from "@mui/material/Box";
import Link from "@mui/material/Link";
import Button from "@mui/material/Button";
import { useEffect, useState } from "react";

export const AuthJwt = ({ method }: { method: AuthMethod }) => {
  const [state, setState] = useState<'new' | 'redirect' | 'validating'>('new');
  const [token, setToken] = useState<string>();
  const [expiresAt, setExpiresAt] = useState<number>();

  useEffect(() => {
    if (hasJwtTokenInUrl()) {
      let params = qs.parse(window.location.hash.substring(1));

      setState('validating')
      setToken(String(params.access_token))
      if (Number(params.expires_in)) {
        setExpiresAt(Date.now() + Number(params.expires_in) * 1000);
      }
    } else {
      setState('redirect')
    }
  }, [])

  if (method.options?.type !== 'oauth2' || !method.options?.authorize_endpoint) {
    return <CredentialsForm usernames={method.usernames} />
  }

  let uri = getRedirectUri(method);


  if (state === 'new') {
    return <></>
  }

  if (state === 'redirect') {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center' }}>
        <Link href={uri}>
          <Button variant="contained">
            Sign in with identity provider
          </Button>
        </Link>
      </Box>
    )
  }

  return <CredentialsForm usernames={method.usernames} password={token} expiresAt={expiresAt} />
};

export const hasJwtTokenInUrl = () => {
  if (!window.location.hash || !window.location.hash.includes('access_token')) {
    return false;
  }

  return true;
}

const getRedirectUri = (method: AuthMethod) => {
  let params = JSON.parse(JSON.stringify(method.options.params || {}))
  for (const key in params) {
    params[key] = params[key].replace('${origin}', window.location.origin)
  }

  return `${method.options!.authorize_endpoint}?${qs.stringify(params)}`
}