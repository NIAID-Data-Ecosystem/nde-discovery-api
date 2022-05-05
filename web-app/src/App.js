import './App.css'
import Home from "./components/Home";
import UI from './components/UI'
import {
  BrowserRouter,
  Routes,
  Route,
} from "react-router-dom";

import { ApolloProvider, InMemoryCache, ApolloClient } from "@apollo/client";
import SourcesUI from './components/SourcesUI';
const client = new ApolloClient({
  uri: "https://release-notes-app-copy-90e.can.canonic.dev/graphql",
  cache: new InMemoryCache(),
});

function App() {
  return (
    <>
      <ApolloProvider client={client}>
        <div className="App" style={{ height: "100vh" }}>
          <BrowserRouter>
            <Routes>
              <Route path='/' element={<Home />} />
              <Route path='/versions' element={
                <UI />
              } />
              <Route path='/sources' element={
                <SourcesUI />
              } />
            </Routes>
          </BrowserRouter>
        </div>
      </ApolloProvider>
    </>
  );
}

export default App;
