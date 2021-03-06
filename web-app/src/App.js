import './App.css'
import Home from "./components/Home";
import UI from './components/UI'
import {
  Routes,
  Route,
  HashRouter,
} from "react-router-dom";

import { ApolloProvider, InMemoryCache, ApolloClient } from "@apollo/client";

const client = new ApolloClient({
  uri: "https://release-notes-app-copy-90e.can.canonic.dev/graphql",
  cache: new InMemoryCache(),
});

function App() {
  return (
    <>
      <ApolloProvider client={client}>
        <div className="App" style={{ height: "100vh" }}>
          <HashRouter>
            <Routes>
              <Route exact path={'/'} element={<Home />} />
              <Route exact path={'/versions'} element={
                <UI />
              } />
            </Routes>
          </HashRouter>
        </div>
      </ApolloProvider>
    </>
  );
}

export default App;
