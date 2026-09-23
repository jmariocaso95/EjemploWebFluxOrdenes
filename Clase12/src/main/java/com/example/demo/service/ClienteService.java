package com.example.demo.service;

import com.example.demo.model.Cliente;
import com.example.demo.repository.ClienteRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ClienteService {
    private final ClienteRepository clienteRepository;

    public ClienteService(ClienteRepository clienteRepository) {
        this.clienteRepository = clienteRepository;
    }

    public Flux<Cliente> getAllClientes() {
        return clienteRepository.findAll();
    }

    public Mono<Cliente> saveCliente(Cliente cliente) {
        var vcliente = Mono.just(cliente);
        return clienteRepository.save(cliente);
    }

    public Flux<Cliente> findClientesByNombre(String nombre) {

        return clienteRepository.findByNombreContaining(nombre);

    }
}
