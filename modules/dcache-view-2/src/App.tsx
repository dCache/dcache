import { useState } from 'react'
import dCacheLogoImg from './assets/dCacheLogo.png'
import './App.css'

function App() {
  const [count, setCount] = useState(0)

  return (
    <>
      <section id="center">
        <div className="hero">
          <img src={dCacheLogoImg} className="base" width="170" height="179" alt="" />
        </div>
        <div>
          <h1>dCache view 2.0</h1>
          <p>
            Edit <code>src/App.tsx</code> and save to test <code>HMR</code>
          </p>
        </div>
        <button
          type="button"
          className="counter"
          onClick={() => setCount((count) => count + 1)}
        >
          Count is {count}
        </button>
      </section>

      <div className="ticks"></div>
    </>
  )
}

export default App
