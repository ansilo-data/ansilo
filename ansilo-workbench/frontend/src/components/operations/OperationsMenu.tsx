import * as React from "react";
import List from "@mui/material/List";
import { useRouter } from "next/router";
import Paper from "@mui/material/Paper";
import Link from "next/link";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import LogsIcon from '@mui/icons-material/Toc';
import DataFlowIcon from '@mui/icons-material/Air';
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
        <Link href="/operations/data-flow">
          <ListItemButton
            selected={router.asPath.startsWith("/operations/data-flow")}
          >
            <ListItemIcon>
              <DataFlowIcon />
            </ListItemIcon>
            <ListItemText primary="Data Flow" />
          </ListItemButton>
        </Link>
        <Link href="/operations/status">
          <ListItemButton
            selected={router.asPath.startsWith("/operations/status")}
          >
            <ListItemIcon>
              <InsertChartIcon />
            </ListItemIcon>
            <ListItemText primary="Status" />
          </ListItemButton>
        </Link>
      </List>
    </Paper>
  );
}
