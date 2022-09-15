import * as React from "react";
import { styled } from "@mui/material/styles";
import MuiDrawer from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import MuiAppBar, { AppBarProps as MuiAppBarProps } from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import List from "@mui/material/List";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import Container from "@mui/material/Container";
import MenuIcon from "@mui/icons-material/Menu";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import { MainMenuItems, SecondaryMenuItems } from "./MenuItems";
import { AuthModal } from "../auth/AuthModal";
import { useState } from "react";
import { clearCredentials, selectAuth, selectCredentials, setModalOpen as setAuthModalOpen } from "../auth/auth.slice";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import Button from "@mui/material/Button";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";

const drawerWidth: number = 240;

interface AppBarProps extends MuiAppBarProps {
  open?: boolean;
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open",
})<AppBarProps>(({ theme, open }) => ({
  zIndex: theme.zIndex.drawer + 1,
  transition: theme.transitions.create(["width", "margin"], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  ...(open && {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(["width", "margin"], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const Drawer = styled(MuiDrawer, {
  shouldForwardProp: (prop) => prop !== "open",
})(({ theme, open }) => ({
  "& .MuiDrawer-paper": {
    position: "relative",
    whiteSpace: "nowrap",
    width: drawerWidth,
    transition: theme.transitions.create("width", {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
    boxSizing: "border-box",
    ...(!open && {
      overflowX: "hidden",
      transition: theme.transitions.create("width", {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
      }),
      width: theme.spacing(7),
      [theme.breakpoints.up("sm")]: {
        width: theme.spacing(9),
      },
    }),
  },
}));

interface TemplateProps {
  title: String;
  children?: React.ReactNode;
}

export const Template = (props: TemplateProps) => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth)
  const creds = useAppSelector(selectCredentials)
  const [open, setOpen] = useState(true);
  const toggleDrawer = () => {
    setOpen(!open);
  };

  const [authMenuAnchorEl, setAuthMenuAnchorEl] = useState<null | HTMLElement>(null);
  const authMenuOpen = Boolean(authMenuAnchorEl);
  const handleAuthClick = (event: React.MouseEvent<HTMLElement>) => {
    setAuthMenuAnchorEl(event.currentTarget);
  };
  const handleAuthClose = () => {
    setAuthMenuAnchorEl(null);
  };
  const handleLogin = () => {
    dispatch(setAuthModalOpen(true))
    handleAuthClose();
  };
  const handleLogout = () => {
    dispatch(clearCredentials(null))
    handleAuthClose();
  };

  return (
    <Box sx={{ display: "flex" }}>
      <AppBar position="absolute" open={open}>
        <AuthModal />
        <Toolbar
          sx={{
            pr: "24px", // keep right padding when drawer closed
          }}
        >
          <IconButton
            edge="start"
            color="inherit"
            aria-label="open drawer"
            onClick={toggleDrawer}
            sx={{
              marginRight: "36px",
              ...(open && { display: "none" }),
            }}
          >
            <MenuIcon />
          </IconButton>
          <Typography
            component="h1"
            variant="h6"
            color="inherit"
            noWrap
            sx={{ flexGrow: 1 }}
          >
            {props.title}
          </Typography>
          <Button
            onClick={handleAuthClick}
          >
            {creds ? `Logged in as ${creds.username}` : 'Login'}
          </Button>
          <Menu
            anchorEl={authMenuAnchorEl}
            open={authMenuOpen}
            onClose={handleAuthClose}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
          >
            {creds
              ? <MenuItem onClick={handleLogout}>Log out</MenuItem> :
              <MenuItem onClick={handleLogin}>Log in</MenuItem>
            }
          </Menu>
        </Toolbar>
      </AppBar>
      <Drawer variant="permanent" open={open}>
        <Toolbar
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            pl: [2],
            pr: [1],
          }}
        >
          <Container
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "left",
              px: [0],
            }}
          >
            <Typography variant="h6" sx={{ fontWeight: '100', lineHeight: '16px' }}>Ansilo</Typography>
          </Container>
          <IconButton onClick={toggleDrawer}>
            <ChevronLeftIcon />
          </IconButton>
        </Toolbar>
        <Divider />
        <List component="nav">
          <MainMenuItems />
          <Divider sx={{ my: 1 }} />
          <SecondaryMenuItems />
        </List>
      </Drawer>
      <Box
        component="main"
        sx={{
          backgroundColor: (theme) =>
            theme.palette.mode === "light"
              ? theme.palette.grey[100]
              : theme.palette.grey[900],
          flexGrow: 1,
          height: "100vh",
          overflow: "auto",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <Toolbar />
        {props.children}
      </Box>
    </Box>
  );
};
