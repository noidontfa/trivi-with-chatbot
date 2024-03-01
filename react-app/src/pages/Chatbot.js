import React, { useEffect, useContext, useState} from 'react';
import { AppContext } from "./AppContext";
import {
  Col,
  Row,
  Form,
  Container,
  Button,
  Modal,
  Table,
  Card,
  Alert, InputGroup
} from "@themesberg/react-bootstrap";
import Markdown from "react-markdown";
import remarkGfm from 'remark-gfm';
import rehypeRaw from "rehype-raw";
import {VegaLite} from "react-vega";
import {
  faUser,
  faRobot,
  faPaperPlane
} from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

const Chatbot = () => {
  const {fetchRequest} = useContext(AppContext);
  const [conversations, setConversations] = useState([
    {
      id: '1',
      who: 'System',
      type: "text",
      content: "Hello! Can i assist you today?"
    }
  ])
  const [prompt, setPrompt] = useState('')

  const [histories, setHistories] = useState([
    {
      input: '',
      output: ''
    },
    {
      input: '',
      output: ''
    }
  ])
  const [disableControl, setDisableControl] = useState(false)
  const handleSubmit = (e) => {
    e.preventDefault();
    if (!prompt) return;
    setConversations([...conversations, {
      id: new Date().getTime(),
      who: 'User',
      type: 'text',
      content: prompt
    }]);
    setDisableControl(true)
    fetchRequest('knowledge/get-conv', 'POST', JSON.stringify({
      prompt_input: prompt,
      histories
    })).then((data) => {
      if (data !== undefined) {
        if (data.status === 200) {
          setPrompt('')

          setConversations((s) => [...s, {
            id: new Date().getTime(),
            who: 'System',
            type: data.type,
            content: data.content
          }]);

          setHistories(data.histories)
        } else {
          setConversations((s) => [...s, {
            id: new Date().getTime(),
            who: 'System',
            type: 'text',
            content: "Something went wrong, please try again"
          }]);
        }
      }
      setPrompt('')
      setDisableControl(false)
    })
  }
  return (
    <article>
      <Container className="px-0">
        <Row className="d-flex flex-wrap flex-md-nowrap align-items-center pt-3">
          <Col className="d-block mb-2 mb-md-0">
            <h1 className="h2">Chatbot</h1>
          </Col>
        </Row>

        <Card className="mb-2" style={{
          minHeight: 'calc(100vh - 250px)'
        }}>
          <Card.Body >
            {conversations.map((result => {
              switch(result.type) {
                case 'text':
                  return (
                    <Row className="d-flex flex-wrap flex-md-nowrap justify-content-center align-items-center" key={result.id}>
                      <Form className="row">
                        <Form.Group className="col-12">
                           <Form.Label><FontAwesomeIcon icon={result.who === 'System' ? faRobot : faUser}/> :</Form.Label>
                          <Markdown remarkPlugins={[[remarkGfm, {singleTilde: false}]]} rehypePlugins={[rehypeRaw]}>
                            {result.content}
                          </Markdown>
                        </Form.Group>
                      </Form>
                    </Row>
                  );
                case 'vega':
                  return (
                    <Row className="d-flex flex-wrap flex-md-nowrap justify-content-center align-items-center" key={result.id}>
                      <Form className="row">
                        <Form.Group className="col-12">
                          <Form.Label><FontAwesomeIcon icon={result.who === 'System' ? faRobot : faUser}/> :</Form.Label>
                        </Form.Group>
                        <VegaLite spec={JSON.parse(result.content)}/>
                      </Form>
                    </Row>
                  );
                default:
                  return <></>;
              }}
            ))
            }
          </Card.Body>
          <Card.Footer>
            <Row className="d-flex flex-wrap flex-md-nowrap justify-content-center align-items-center py-3">
             <Form className="row" onSubmit={handleSubmit}>
                <InputGroup className="col-12">
                    <Form.Control type="text" value ={prompt} onChange={e => setPrompt(e.target.value)} disabled={disableControl}/>
                    <InputGroup.Text style={{
                      borderRight: '0.0625rem solid #d1d7e0'
                    }}>
                      <Button variant="success" disabled={disableControl} type={'submit'}>
                        <FontAwesomeIcon icon={faPaperPlane}/>
                      </Button>
                    </InputGroup.Text>
                </InputGroup>
              </Form>
            </Row>
          </Card.Footer>
        </Card>
      </Container>
    </article>
  );
};

export default Chatbot;