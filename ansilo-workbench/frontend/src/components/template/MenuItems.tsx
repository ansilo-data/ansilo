import * as React from "react";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListAlt from '@mui/icons-material/ListAlt';
import AccountTree from "@mui/icons-material/AccountTree";
import ModelTraining from "@mui/icons-material/ModelTraining";
import QueryStats from "@mui/icons-material/QueryStats";
import VerifiedUser from "@mui/icons-material/VerifiedUserOutlined";
import SettingsApplicationsOutlined from "@mui/icons-material/SettingsApplicationsOutlined";
import { useTheme } from "@mui/material/styles";
import Link from "next/link";
import { useRouter } from "next/router";

export const MainMenuItems = () => {
  const theme = useTheme();
  const router = useRouter();

  return (
    <React.Fragment>
      <Link href="/catalog">
        <ListItemButton selected={router.asPath.startsWith("/catalog")}>
          <ListItemIcon>
            <ListAlt />
          </ListItemIcon>
          <ListItemText primary="Data Catalog" />
        </ListItemButton>
      </Link>
      <Link href="/workbench">
        <ListItemButton selected={router.asPath.startsWith("/workbench")}>
          <ListItemIcon>
            <QueryStats />
          </ListItemIcon>
          <ListItemText primary="Workbench" />
        </ListItemButton>
      </Link>
      <Link href="/jobs">
        <ListItemButton selected={router.asPath.startsWith("/jobs")}>
          <ListItemIcon>
            <AccountTree />
          </ListItemIcon>
          <ListItemText primary="Jobs" />
        </ListItemButton>
      </Link>
    </React.Fragment>
  );
};

export const SecondaryMenuItems = () => {
  const router = useRouter();

  return (
    <React.Fragment>
      <Link href="/governance">
        <ListItemButton selected={router.asPath.startsWith("/governance")}>
          <ListItemIcon>
            <VerifiedUser />
          </ListItemIcon>
          <ListItemText primary="Governance" />
        </ListItemButton>
      </Link>
      <Link href="/operations">
        <ListItemButton selected={router.asPath.startsWith("/operations")}>
          <ListItemIcon>
            <SettingsApplicationsOutlined />
          </ListItemIcon>
          <ListItemText primary="Operations" />
        </ListItemButton>
      </Link>
    </React.Fragment>
  );
};
