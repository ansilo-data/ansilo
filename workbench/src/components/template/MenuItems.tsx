import * as React from "react";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListAlt from "@mui/icons-material/ListALt";
import AccountTree from "@mui/icons-material/AccountTree";
import ModelTraining from "@mui/icons-material/ModelTraining";
import QueryStats from "@mui/icons-material/QueryStats";
import VerifiedUser from "@mui/icons-material/VerifiedUserOutlined";
import SettingsApplicationsOutlined from "@mui/icons-material/SettingsApplicationsOutlined";
import { useTheme } from "@mui/material/styles";
import Link from "next/link";

export const MainMenuItems = () => {
  const theme = useTheme();

  return (
    <React.Fragment>
      <Link href="/catalog">
        <ListItemButton>
          <ListItemIcon>
            <ListAlt />
          </ListItemIcon>
          <ListItemText primary="Data Catalog" />
        </ListItemButton>
      </Link>
      <ListItemButton>
        <ListItemIcon>
          <QueryStats />
        </ListItemIcon>
        <ListItemText primary="Workbench" />
      </ListItemButton>
      <ListItemButton>
        <ListItemIcon>
          <AccountTree />
        </ListItemIcon>
        <ListItemText primary="Jobs" />
      </ListItemButton>
      <ListItemButton>
        <ListItemIcon>
          <ModelTraining />
        </ListItemIcon>
        <ListItemText
          primary={
            <>
              Streams{" "}
              <small style={{ color: theme.palette.grey[500] }}>(soon)</small>
            </>
          }
        />
      </ListItemButton>
    </React.Fragment>
  );
};

export const SecondaryMenuItems = () => {
  return (
    <React.Fragment>
      <ListItemButton>
        <ListItemIcon>
          <VerifiedUser />
        </ListItemIcon>
        <ListItemText primary="Governance" />
      </ListItemButton>
      <ListItemButton>
        <ListItemIcon>
          <SettingsApplicationsOutlined />
        </ListItemIcon>
        <ListItemText primary="Operations" />
      </ListItemButton>
    </React.Fragment>
  );
};
