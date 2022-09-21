import * as React from "react";
import List from "@mui/material/List";
import { useRouter } from "next/router";
import Paper from "@mui/material/Paper";
import Link from "next/link";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import AccountBalanceIcon from '@mui/icons-material/AccountBalance';
import PrecisionManufacturingIcon from '@mui/icons-material/PrecisionManufacturing';
import GppGoodIcon from '@mui/icons-material/GppGood';
import BadgeIcon from '@mui/icons-material/Badge';

const navigationWidth = 240;

interface Props {
}

export default function GovernanceMenu(props: Props) {
  const router = useRouter();

  return (
    <Paper sx={{ maxWidth: navigationWidth, flexGrow: 1 }} elevation={6}>
      <List component="nav">
        <Link href="/governance/authorities">
          <ListItemButton
            selected={router.asPath.startsWith("/governance/authorities")}
          >
            <ListItemIcon>
              <AccountBalanceIcon />
            </ListItemIcon>
            <ListItemText primary="Authorities" />
          </ListItemButton>
        </Link>
        <Link href="/governance/roles">
          <ListItemButton
            selected={router.asPath.startsWith("/governance/roles")}
          >
            <ListItemIcon>
              <BadgeIcon />
            </ListItemIcon>
            <ListItemText primary="Roles" />
          </ListItemButton>
        </Link>
        <Link href="/governance/service-users">
          <ListItemButton
            selected={router.asPath.startsWith("/governance/service-users")}
          >
            <ListItemIcon>
              <PrecisionManufacturingIcon />
            </ListItemIcon>
            <ListItemText primary="Service Users" />
          </ListItemButton>
        </Link>
      </List>
    </Paper>
  );
}
