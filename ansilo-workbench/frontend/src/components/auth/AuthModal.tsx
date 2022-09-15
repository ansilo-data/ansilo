import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
import { useEffect, useState } from "react";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import { AuthMethod, clearCredentials, fetchAuthMethodsAsync, selectAuth, selectCredentials, setCredentials, setMethodId, setModalOpen as setAuthModalOpen } from "./auth.slice";
import TextField from "@mui/material/TextField";
import MenuItem from "@mui/material/MenuItem";
import { AuthUsernamePassword } from "./AuthUsernamePassword";
import { AuthJwt, hasJwtTokenInUrl } from "./AuthJwt";
import { AuthSaml } from "./AuthSaml";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import IconButton from "@mui/material/IconButton";
import CloseIcon from '@mui/icons-material/Close';
import { useRouter } from "next/router";

export const AuthModal = () => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth);
  const creds = useAppSelector(selectCredentials);
  const router = useRouter();

  useEffect(() => {
    dispatch(fetchAuthMethodsAsync())

    if (hasJwtTokenInUrl()) {
      dispatch(setAuthModalOpen(true))
    }
  }, [])

  if (!auth.methods) {
    return <></>
  }

  const handleClose = () => {
    if (!auth.modalOpen) {
      router.push('/catalog');
    }
    dispatch(setAuthModalOpen(false))
  }

  return (
    <Dialog
      open={auth.modalOpen || (auth.authRequiredSem > 0 && !creds)}
      onClose={handleClose}
    >
      <DialogTitle>
        <Box sx={{ 'display': 'flex', justifyContent: 'space-between' }}>
          <span>Login</span>
          <IconButton onClick={handleClose}>
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent
        sx={{ width: 600, maxWidth: '90vw' }}
      >
        <Box sx={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
          {auth.methods ? <AuthMethods /> : <CircularProgress />}
        </Box>
      </DialogContent>
    </Dialog>
  );
};

const AuthMethods = () => {
  const dispatch = useAppDispatch()
  const auth = useAppSelector(selectAuth);

  return (
    <Box
      sx={{
        display: "flex",
        pt: 4,
        flexDirection: "column",
        "& > *:not(:last-child)": { pb: 2 },
      }}
    >
      <TextField
        required
        select
        label="Select Login Method"
        value={auth.methodId || ''}
        onChange={(e) => dispatch(setMethodId(e.target.value))}
      >
        {auth.methods!.map((m) => (
          <MenuItem key={m.id} value={m.id}>
            Login with {m.name}
          </MenuItem>
        ))}
      </TextField>
      {auth.methodId ? <AuthMethod method={auth.methods!.find(i => i.id === auth.methodId)!} /> : undefined}
    </Box>
  );
};

const AuthMethod = ({ method }: { method: AuthMethod }) => {
  const auth = useAppSelector(selectAuth);

  if (method.type === 'username_password') {
    return <AuthUsernamePassword />
  }

  if (method.type === 'jwt') {
    return <AuthJwt method={method} />
  }

  if (method.type === 'saml') {
    return <AuthSaml method={method} />
  }

  return <AuthUsernamePassword />
};
