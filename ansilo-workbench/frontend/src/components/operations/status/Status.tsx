import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import OperationsMenu from "../OperationsMenu";
import { useAppDispatch, useAppSelector } from "../../../store/hooks";
import { useEffect, useState } from "react";
import { API_CONFIG } from "../../../config/api";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import AlertTitle from "@mui/material/AlertTitle";
import CircularProgress from "@mui/material/CircularProgress";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableCell from "@mui/material/TableCell";
import TableBody from "@mui/material/TableBody";

interface Status {
  ok: boolean
  error?: string
}

interface VersionInfo {
  version?: string,
  builtAt?: Date
  error?: string
}

export const Status = () => {
  const dispatch = useAppDispatch();
  const [status, setStatus] = useState<Status>();
  const [versionInfo, setVersionInfo] = useState<VersionInfo>();

  useEffect(() => {
    (async () => {
      fetch(`${API_CONFIG.origin}/api/health`).then(r => {
        if (r.status >= 400) {
          throw new Error(`Unexpected status code from health check: ${r.status}`)
        }
        return r.text();
      })
        .then(msg => {
          if (!msg.includes("Ok")) {
            throw new Error(`Unexpected health check response: ${msg}`)
          }
        })
        .then(() => {
          setStatus({ ok: true })
        })
        .catch(e => {
          setStatus({ ok: false, error: String(e) })
        })

      fetch(`${API_CONFIG.origin}/api/version`).then(r => r.json())
        .then(res => setVersionInfo({
          version: res.version,
          builtAt: new Date(res.built_at)
        }))
        .catch(e => setVersionInfo({ error: `Failed to fetch version info: ${e}` }))
    })()
  }, [])

  return (
    <Box sx={{ flexGrow: "1", display: "flex" }}>
      <OperationsMenu />
      <Container
        sx={{
          maxWidth: 800,
          flexGrow: 1,
          display: "flex",
          justifyContent: "center",
          padding: 4,
        }}
      >
        <Paper
          sx={{ display: "flex", p: 4, flexDirection: "column", minWidth: 400 }}
          elevation={8}
        >
          <Typography sx={{ mb: 2 }} variant="h6">
            System Status
          </Typography>
          {status !== undefined ?
            <Alert severity={status.error ? 'error' : 'success'} sx={{ mb: 4 }}>
              <AlertTitle>System Status</AlertTitle>
              {status.error ? status.error : 'System is operating nominally'}
            </Alert>
            : <CircularProgress sx={{ mh: 'auto', mb: 4 }} />}
          <Typography sx={{ mb: 2 }} variant="h6">
            Version
          </Typography>
          {versionInfo !== undefined ?
            (versionInfo.error ?
              <Alert severity="error" sx={{ mb: 4 }}>
                <AlertTitle>Error</AlertTitle>
                {versionInfo.error}
              </Alert>
              : <TableContainer>
                <Table stickyHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell>System Version</TableCell>
                      <TableCell>{versionInfo.version}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Database Build Time</TableCell>
                      <TableCell>{versionInfo.builtAt?.toLocaleString()}</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableContainer>)
            : <CircularProgress sx={{ mh: 'auto', mb: 4 }} />}
        </Paper>
      </Container>
    </Box>
  );
};
