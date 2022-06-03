import { createTheme } from "@mui/material/styles";

// Create a theme instance.
const theme = createTheme({
  palette: {
    mode: "dark",
  },
  typography: {
    fontWeightRegular: 100,
    h6: {
      fontWeight: 100,
    },
  },
});

export default theme;
