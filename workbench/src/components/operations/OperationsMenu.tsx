import * as React from "react";
import List from "@mui/material/List";
import { useRouter } from "next/router";
import Paper from "@mui/material/Paper";
import Link from "next/link";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import LogsIcon from '@mui/icons-material/Toc';
import InsertChartIcon from '@mui/icons-material/InsertChart';
import NotificationsActiveIcon from '@mui/icons-material/NotificationsActive';

const navigationWidth = 240;

interface Props {
}

export default function OperationsMenu(props: Props) {
  const router = useRouter();

  return (
    <Paper sx={{ maxWidth: navigationWidth, flexGrow: 1 }} elevation={6}>
      <List component="nav">
        <Link href="/operations/logs">
          <ListItemButton
            selected={router.asPath.startsWith("/operations/logs")}
          >
            <ListItemIcon>
              <LogsIcon />
            </ListItemIcon>
            <ListItemText primary="Logs" />
          </ListItemButton>
        </Link>
        <Link href="/operations/metrics">
          <ListItemButton
            selected={router.asPath.startsWith("/operations/metrics")}
          >
            <ListItemIcon>
              <InsertChartIcon />
            </ListItemIcon>
            <ListItemText primary="Metrics" />
          </ListItemButton>
        </Link>
        <Link href="/operations/alerts">
          <ListItemButton
            selected={router.asPath.startsWith("/operations/alerts")}
          >
            <ListItemIcon>
              <NotificationsActiveIcon />
            </ListItemIcon>
            <ListItemText primary="Alerts" />
          </ListItemButton>
        </Link>
      </List>
    </Paper>
  );
}
