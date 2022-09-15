import Box from "@mui/material/Box";
import { useEffect, useState } from "react";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import { AuthCredentials, selectAuth, setCredentials } from "./auth.slice";
import TextField from "@mui/material/TextField";
import LoadingButton from "@mui/lab/LoadingButton";
import { validateCredentials } from "./auth.api";
import Alert from "@mui/material/Alert";
import AlertTitle from "@mui/material/AlertTitle";
import MenuItem from "@mui/material/MenuItem";

export interface CredentialsFormProps {
  usernames?: string[],
  usernameLabel?: string,
  password?: string,
  passwordLabel?: string,
  expiresAt?: number
}

export const CredentialsForm = (props: CredentialsFormProps) => {
  const dispatch = useAppDispatch()
  const auth = useAppSelector(selectAuth);

  const usernameLabel = props.usernameLabel || 'Username';
  const passwordLabel = props.passwordLabel || 'Password';

  const [username, setUsername] = useState<string>(props.usernames ? props.usernames[0] : '');
  const [password, setPassword] = useState<string>(props.password || '');
  const [error, setError] = useState<string>();
  const [authenticating, setAuthenticating] = useState<boolean>(false);

  const handleSubmit = async () => {
    setAuthenticating(true);
    setError('')

    let creds: AuthCredentials = { username: username!, password: password!, expiresAt: props.expiresAt };

    try {
      if (await validateCredentials(creds)) {
        dispatch(setCredentials(creds))
      } else {
        setError("Credentials are invalid")
      }
    } catch (e) {
      console.warn(`Authentication error: `, e)
      setError(`Error occurred: ${e}`)
    }

    setAuthenticating(false);
  }

  useEffect(() => {
    setTimeout(() => {
      if (props.usernames?.length === 1 && props.password) {
        handleSubmit()
      }
    }, 0);
  }, []);

  return (
    <Box
      component="form"
      sx={{
        pt: 4,
        display: "flex",
        flexDirection: "column",
      }}>
      {error && <Alert severity="error" sx={{ mb: 4 }}>
        <AlertTitle>Error</AlertTitle>
        {error}
      </Alert>}
      <TextField
        required
        label={usernameLabel}
        value={username}
        select={!!props.usernames?.length}
        onChange={(e) =>
          setUsername(e.target.value)
        }
        sx={{ pb: 2 }}
      >
        {(props.usernames || []).map((username) => (
          <MenuItem key={username} value={username}>
            {username}
          </MenuItem>
        ))}
      </TextField>
      {props.password ? undefined : <TextField
        required
        type="password"
        label={passwordLabel}
        value={password}
        onChange={(e) =>
          setPassword(e.target.value)
        }
        sx={{ pb: 2 }}
      />}
      <LoadingButton
        type="submit"
        variant="contained"
        onClick={handleSubmit}
        loading={authenticating}
        sx={{ maxWidth: 200, ml: 'auto' }}
      >
        Login
      </LoadingButton>
    </Box>
  )
};